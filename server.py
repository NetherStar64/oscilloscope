import socket
import struct
import threading
import queue
import time
import contextlib
import numpy as np
import pyqtgraph as pg
from PyQt6 import QtCore, QtWidgets

PORT = 4444
CONTROL_PORT = 4445
CAPTURE_PORT = 4446
STATUS_PORT = 4447
VOLTAGE_PORT = 4448
STATUS_FLASH_MIN_INTERVAL = 1 / 60
STATUS_FLASH_DURATION_MS = 100
TOTAL_SAMPLES = 1024
SAMPLES_PER_PACKET = 512
NUM_PARTS = TOTAL_SAMPLES // SAMPLES_PER_PACKET
NUM_RING_BUFFERS = 84
SAMPLE_BUFFER_SIZE = 1024
MAX_CAPTURE_SAMPLES = NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE

NOMINAL_ADC_SPS = 500_000
MIN_ADC_SPS = 733
# HISTORY_SEC = 0.1
HISTORY_SEC = (TOTAL_SAMPLES*2) / NOMINAL_ADC_SPS
HISTORY_SAMPLES = int(HISTORY_SEC * NOMINAL_ADC_SPS)

# 3.3V ADC Max + 1/2 Voltage divider -> VSCALE = Voltage at Max ADC
VSCALE = 3.3 / (1/2)

READOUT_CARD_STYLE = """
    QFrame {
        background-color: #242424;
        border: 1px solid #555555;
        border-radius: 8px;
    }
"""

frame_queue = queue.Queue(maxsize=100)
capture_queue = queue.Queue()

stats_lock = threading.Lock()
rx_frame_count = 0
rx_byte_count = 0
viewer_address = None
worker_stop = threading.Event()
worker_socket_lock = threading.Lock()
worker_sockets = set()
worker_threads = []
device_seen_lock = threading.Lock()
last_device_seen_signal = 0.0

class StatusEvents(QtCore.QObject):
    trigger_hit = QtCore.pyqtSignal(int)
    overflow = QtCore.pyqtSignal(int)
    once_state = QtCore.pyqtSignal(bool)
    worker_error = QtCore.pyqtSignal(str)
    device_seen = QtCore.pyqtSignal(str)
    reset_complete = QtCore.pyqtSignal()

status_events = StatusEvents()

class VoltageEvents(QtCore.QObject):
    reading = QtCore.pyqtSignal(int, int)

voltage_events = VoltageEvents()

def register_worker_socket(sock):
    with worker_socket_lock:
        if worker_stop.is_set():
            sock.close()
            raise OSError("Viewer is shutting down")
        worker_sockets.add(sock)

def close_worker_socket(sock):
    if sock is None:
        return
    with worker_socket_lock:
        worker_sockets.discard(sock)
    with contextlib.suppress(OSError):
        sock.close()

def report_worker_error(worker_name, error):
    if not worker_stop.is_set():
        status_events.worker_error.emit(f"{worker_name} failed: {error}")

def notify_device_seen(address):
    global viewer_address, last_device_seen_signal
    viewer_address = address
    now = time.monotonic()
    with device_seen_lock:
        if now - last_device_seen_signal < 0.5:
            return
        last_device_seen_signal = now
    status_events.device_seen.emit(address)

def udp_worker():
    global rx_frame_count, rx_byte_count
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", PORT))
        sock.settimeout(0.5)
        register_worker_socket(sock)
        frames = {}

        while not worker_stop.is_set():
            try:
                packet, sender = sock.recvfrom(2048)
            except socket.timeout:
                continue
            if len(packet) < 5 + SAMPLES_PER_PACKET * 2:
                continue
            sample_count, sample_part = struct.unpack_from("<IB", packet, 0)
            if sample_part >= NUM_PARTS:
                continue
            notify_device_seen(sender[0])
            data_offset = len(packet) - (SAMPLES_PER_PACKET * 2)
            samples = np.frombuffer(
                packet, dtype=np.uint16, offset=data_offset,
                count=SAMPLES_PER_PACKET
            )

            if sample_count not in frames:
                frames[sample_count] = {}
            frames[sample_count][sample_part] = samples

            if len(frames[sample_count]) == NUM_PARTS:
                full_frame = np.concatenate(
                    [frames[sample_count][part] for part in range(NUM_PARTS)]
                )
                del frames[sample_count]
                with stats_lock:
                    rx_frame_count += 1
                    rx_byte_count += len(packet) * NUM_PARTS
                if frame_queue.full():
                    with contextlib.suppress(queue.Empty):
                        frame_queue.get_nowait()
                frame_queue.put(full_frame)

            if len(frames) > 20:
                del frames[min(frames.keys())]
    except OSError as error:
        report_worker_error("Sample UDP listener", error)
    finally:
        close_worker_socket(sock)

def recv_exact(connection, length):
    data = bytearray(length)
    view = memoryview(data)
    offset = 0
    while offset < length:
        try:
            received = connection.recv_into(view[offset:], length - offset)
        except socket.timeout:
            if worker_stop.is_set():
                return None
            continue
        if not received:
            return None
        offset += received
    return data

def recv_samples(connection, sample_count):
    samples = np.empty(sample_count, dtype="<u2")
    payload = memoryview(samples).cast("B")
    offset = 0
    while offset < len(payload):
        try:
            received = connection.recv_into(payload[offset:], len(payload) - offset)
        except socket.timeout:
            if worker_stop.is_set():
                return None
            continue
        if not received:
            return None
        offset += received
    return samples

def recv_packed_samples(connection, sample_count):
    payload_length = (sample_count // 2) * 3 + (sample_count % 2) * 2
    packed = bytearray(payload_length)
    view = memoryview(packed)
    offset = 0
    while offset < payload_length:
        try:
            received = connection.recv_into(view[offset:], payload_length - offset)
        except socket.timeout:
            if worker_stop.is_set():
                return None
            continue
        if not received:
            return None
        offset += received

    samples = np.empty(sample_count, dtype="<u2")
    pairs = sample_count // 2
    if pairs:
        data = np.frombuffer(packed, dtype=np.uint8, count=pairs * 3)
        data = data.reshape(-1, 3).astype(np.uint16)
        samples[0:2 * pairs:2] = data[:, 0] | ((data[:, 1] & 0x0f) << 8)
        samples[1:2 * pairs:2] = (data[:, 1] >> 4) | (data[:, 2] << 4)
    if sample_count % 2:
        samples[-1] = packed[-2] | (packed[-1] << 8)
    return samples

def tcp_capture_worker():
    server = None
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            server.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        server.bind(("0.0.0.0", CAPTURE_PORT))
        server.listen()
        server.settimeout(0.5)
        register_worker_socket(server)

        while not worker_stop.is_set():
            try:
                connection, _ = server.accept()
            except socket.timeout:
                continue
            except OSError:
                if worker_stop.is_set():
                    break
                raise
            try:
                connection.settimeout(0.5)
                register_worker_socket(connection)
                with connection:
                    handle_capture_connection(connection)
            except OSError as error:
                if not worker_stop.is_set():
                    capture_queue.put(
                        (None, 0, 0, None, 0, 0.0, 0,
                         f"Capture TCP connection reset: {error}")
                    )
            finally:
                close_worker_socket(connection)
    except OSError as error:
        report_worker_error("Capture TCP listener", error)
    finally:
        close_worker_socket(server)

def handle_capture_connection(connection):
    while not worker_stop.is_set():
        transfer_started = time.perf_counter()
        prefix = recv_exact(connection, 4)
        if prefix is None:
            break
        if prefix == b"OS13":
            header_size = 20
        elif prefix in (b"OSCP", b"OS12"):
            header_size = 16
        else:
            capture_queue.put(
                (None, 0, 0, None, 0, 0.0, 0,
                 "Invalid capture header from oscilloscope")
            )
            break
        header_tail = recv_exact(connection, header_size)
        if header_tail is None:
            if not worker_stop.is_set():
                capture_queue.put(
                    (None, 0, 0, None, 0, 0.0, 0,
                     "Incomplete TCP capture header")
                )
            break
        header = prefix + header_tail
        if prefix == b"OS13":
            (magic, sample_count, buffer_capacity, overflow_count,
             trigger_index, sample_rate) = struct.unpack("<4sIIIII", header)
        else:
            (magic, sample_count, buffer_capacity, overflow_count,
             trigger_index) = struct.unpack("<4sIIII", header)
            sample_rate = 0
        if (
            not SAMPLE_BUFFER_SIZE <= buffer_capacity <= MAX_CAPTURE_SAMPLES
            or sample_count > buffer_capacity
            or (sample_count and trigger_index != 0xFFFFFFFF
                and trigger_index >= sample_count)
            or (magic == b"OS13" and
                not 8 <= sample_rate <= NOMINAL_ADC_SPS)
        ):
            capture_queue.put(
                (None, 0, 0, None, 0, 0.0, 0,
                 "Invalid capture header from oscilloscope")
            )
            break

        if sample_count == 0:
            capture_queue.put(
                (None, buffer_capacity, overflow_count, trigger_index,
                 sample_rate, 0.0, 0, None)
            )
            continue

        payload_length = (
            (sample_count // 2) * 3 + (sample_count % 2) * 2
            if magic in (b"OS12", b"OS13") else sample_count * 2
        )
        receive_samples = (
            recv_packed_samples
            if magic in (b"OS12", b"OS13") else recv_samples
        )
        samples = receive_samples(connection, sample_count)
        if samples is None:
            if not worker_stop.is_set():
                capture_queue.put(
                    (None, 0, 0, None, 0, 0.0, 0,
                     "Incomplete TCP capture received")
                )
            break
        transfer_seconds = time.perf_counter() - transfer_started
        capture_queue.put(
            (samples, buffer_capacity, overflow_count, trigger_index,
             sample_rate, transfer_seconds, payload_length, None)
        )

def status_worker():
    udp_event_worker(STATUS_PORT, "Trigger status UDP listener", handle_status_packet)

def voltage_worker():
    udp_event_worker(VOLTAGE_PORT, "ADC voltage UDP listener", handle_voltage_packet)

def udp_event_worker(port, worker_name, packet_handler):
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", port))
        sock.settimeout(0.5)
        register_worker_socket(sock)
        while not worker_stop.is_set():
            try:
                packet, sender = sock.recvfrom(64)
            except socket.timeout:
                continue
            if packet_handler(packet):
                notify_device_seen(sender[0])
    except OSError as error:
        report_worker_error(worker_name, error)
    finally:
        close_worker_socket(sock)

def handle_status_packet(packet):
    if len(packet) != 9 or packet[:4] != b"OSCE":
        return False
    event, value = struct.unpack_from("<BI", packet, 4)
    if event == 1:
        status_events.trigger_hit.emit(value)
    elif event == 2:
        status_events.overflow.emit(value)
    elif event == 3:
        status_events.once_state.emit(bool(value))
    elif event == 4:
        status_events.reset_complete.emit()
    else:
        return False
    return True

def handle_voltage_packet(packet):
    if len(packet) != 8 or packet[:4] != b"OSCV":
        return False
    average, sample_count = struct.unpack_from("<HH", packet, 4)
    if average <= 4095 and sample_count > 0:
        voltage_events.reading.emit(average, sample_count)
        return True
    return False

def stop_workers():
    worker_stop.set()
    with worker_socket_lock:
        sockets = tuple(worker_sockets)
        worker_sockets.clear()
    for sock in sockets:
        with contextlib.suppress(OSError):
            sock.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(OSError):
            sock.close()
    current_thread = threading.current_thread()
    for thread in worker_threads:
        if thread is not current_thread:
            thread.join(timeout=1.0)

class LivePlot(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Oscilloscope")
        self.resize(1180, 680)
        self.setMinimumSize(900, 540)

        self.history_samples = HISTORY_SAMPLES
        self.history = np.zeros(self.history_samples, dtype=np.uint16)
        self.x_data = np.linspace(-HISTORY_SEC, 0.0, self.history_samples)
        self.history_sample_rate = NOMINAL_ADC_SPS
        self.last_raw_capture = None

        self.plot_widget = pg.PlotWidget()
        self.plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self.plot_widget.setYRange(0, 4096)
        self.plot_widget.setXRange(-HISTORY_SEC, 0.0)
        self.plot_widget.setLabel("bottom", "Time", units="s")
        self.plot_widget.setLabel("left", "Voltage", units="V")
        self.plot_widget.getAxis("left").setScale(VSCALE / 4096.0)
        self.curve = self.plot_widget.plot(pen=pg.mkPen(color="#00ffff", width=1.5))
        self.trigger_line = pg.InfiniteLine(
            angle=0, movable=True,
            pen=pg.mkPen(color="#ff8800", width=1, style=QtCore.Qt.PenStyle.DashLine)
        )
        self.trigger_line.setHoverPen(
            pg.mkPen(color="#ffcc66", width=2, style=QtCore.Qt.PenStyle.DashLine)
        )
        self.trigger_line.setBounds((0, 4096))
        self.trigger_line.setToolTip("Drag to adjust the trigger level")
        self.trigger_line.setZValue(10)
        self.plot_widget.addItem(self.trigger_line)

        self.spectrum_widget = pg.PlotWidget()
        self.spectrum_widget.showGrid(x=True, y=True, alpha=0.3)
        self.spectrum_widget.setLabel("bottom", "Frequency", units="Hz")
        self.spectrum_widget.setLabel("left", "Amplitude", units="V peak")
        self.spectrum_widget.disableAutoRange()
        self.spectrum_widget.setXRange(0.0, NOMINAL_ADC_SPS / 2.0, padding=0)
        self.spectrum_widget.setYRange(0.0, VSCALE / 2.0, padding=0)
        self.spectrum_curve = self.spectrum_widget.plot(
            pen=pg.mkPen(color="#00ffff", width=1.5)
        )
        self.plot_stack = QtWidgets.QStackedWidget()
        self.plot_stack.addWidget(self.plot_widget)
        self.plot_stack.addWidget(self.spectrum_widget)

        self.plot_view = QtWidgets.QComboBox()
        self.plot_view.addItems(["Time domain", "FFT spectrum"])
        self.plot_view.setToolTip(
            "FFT shows single-sided peak amplitude with the DC component removed. "
            "Use the mouse wheel and drag to zoom and pan; the scale stays fixed "
            "as new captures arrive."
        )
        self.client_average = QtWidgets.QComboBox()
        for factor in (1, 2, 4, 8, 16, 32, 64):
            self.client_average.addItem(f"{factor}×", factor)
        self.client_average.setToolTip(
            "Average each group of samples on the PC before plotting. "
            "This affects only the display, not device acquisition or saved captures."
        )

        self.mode, self.mode_group, self.mode_buttons = (
            self.create_choice_buttons(("Live view", "Capture on trigger"))
        )
        self.mode_group.button(0).setChecked(True)
        self.sample_rate = QtWidgets.QComboBox()
        self.sample_rate_presets = (
            1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000,
            200_000, 500_000,
        )
        for rate in self.sample_rate_presets:
            self.sample_rate.addItem(f"{rate // 1000:g} kS/s", rate)
        self.sample_rate.addItem("Custom rate...", None)
        self.sample_rate.setToolTip(
            "Choose a preset rate or select Custom rate... to enter any supported rate."
        )
        self.sample_rate.setCurrentIndex(
            self.sample_rate.findData(NOMINAL_ADC_SPS)
        )
        self.selected_sample_rate = NOMINAL_ADC_SPS
        self.trigger_voltage = QtWidgets.QDoubleSpinBox()
        self.trigger_voltage.setRange(0.0, VSCALE)
        self.trigger_voltage.setDecimals(3)
        self.trigger_voltage.setSingleStep(0.01)
        self.trigger_voltage.setValue(VSCALE / 2)
        self.trigger_voltage.setSuffix(" V")
        self.trigger_voltage.setToolTip(
            "Set the trigger level here or drag the orange line on the plot."
        )
        self.edge = QtWidgets.QComboBox()
        self.edge.addItems(["Rising edge", "Falling edge"])
        self.trigger_repeat, self.trigger_repeat_group, self.trigger_repeat_buttons = (
            self.create_choice_buttons(("Once", "Continuous"))
        )
        self.trigger_repeat_group.button(0).setChecked(True)
        self.trigger_offset = QtWidgets.QSpinBox()
        self.trigger_offset.setRange(0, 100)
        self.trigger_offset.setValue(50)
        self.trigger_offset.setSuffix("% before trigger")
        self.capture_length_ms = QtWidgets.QDoubleSpinBox()
        self.capture_length_ms.setDecimals(3)
        self.capture_length_ms.setRange(
            SAMPLE_BUFFER_SIZE / NOMINAL_ADC_SPS * 1000,
            MAX_CAPTURE_SAMPLES / NOMINAL_ADC_SPS * 1000,
        )
        self.capture_length_ms.setSingleStep(
            SAMPLE_BUFFER_SIZE / NOMINAL_ADC_SPS * 1000
        )
        self.capture_length_ms.setValue(
            MAX_CAPTURE_SAMPLES / NOMINAL_ADC_SPS * 1000
        )
        self.capture_length_ms.setSuffix(" ms")
        self.capture_length_ms.setToolTip(
            "Capture duration is rounded up to a whole 1,024-sample DMA block. "
            "The maximum duration depends on sample rate because the device has "
            "a fixed sample-buffer capacity."
        )
        self.last_capture_length = 0
        self.last_capture_overflows = 0
        self.last_capture_seconds = 0.0
        self.last_trigger_flash_time = 0.0
        self.last_overflow_flash_time = 0.0
        self.capture_count = 0
        self.capture_rate = 0.0

        acquisition_group = QtWidgets.QGroupBox("Acquisition")
        acquisition_form = QtWidgets.QFormLayout(acquisition_group)
        acquisition_form.setLabelAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        acquisition_form.addRow(self.mode)
        acquisition_form.addRow("Sample rate", self.sample_rate)
        acquisition_form.addRow("Capture length", self.capture_length_ms)

        trigger_group = QtWidgets.QGroupBox("Trigger")
        trigger_form = QtWidgets.QFormLayout(trigger_group)
        trigger_form.setLabelAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        trigger_form.addRow("Repeat", self.trigger_repeat)
        trigger_form.addRow("Level", self.trigger_voltage)
        trigger_form.addRow("Edge", self.edge)
        trigger_form.addRow("Pre-trigger", self.trigger_offset)

        visualization_group = QtWidgets.QGroupBox("Visualization")
        visualization_form = QtWidgets.QFormLayout(visualization_group)
        visualization_form.setLabelAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        visualization_form.addRow("Plot", self.plot_view)
        visualization_form.addRow("Client average", self.client_average)

        voltage_card = QtWidgets.QFrame()
        voltage_card.setStyleSheet(READOUT_CARD_STYLE)
        voltage_layout = QtWidgets.QVBoxLayout(voltage_card)
        voltage_layout.setContentsMargins(12, 8, 12, 8)
        voltage_layout.setSpacing(2)
        voltage_title = QtWidgets.QLabel("LIVE INPUT")
        voltage_title.setStyleSheet("color: #aaaaaa; font-size: 9pt;")
        self.live_voltage_label = QtWidgets.QLabel("--.--- V")
        self.live_voltage_label.setMinimumHeight(30)
        self.live_voltage_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.live_voltage_label.setStyleSheet(
            "color: #00e5cf; font-size: 20px; font-weight: bold; border: none;"
        )
        self.live_voltage_detail = QtWidgets.QLabel("ADC -- / 4095  |  avg --")
        self.live_voltage_detail.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.live_voltage_detail.setStyleSheet(
            "color: #aaaaaa; font-size: 9pt; border: none;"
        )
        for readout in (self.live_voltage_label, self.live_voltage_detail):
            readout.setSizePolicy(
                QtWidgets.QSizePolicy.Policy.Ignored,
                QtWidgets.QSizePolicy.Policy.Preferred,
            )
        voltage_layout.addWidget(voltage_title)
        voltage_layout.addWidget(self.live_voltage_label)
        voltage_layout.addWidget(self.live_voltage_detail)

        self.capture_once_button = QtWidgets.QPushButton("Trigger arm")
        self.capture_once_button.setMinimumHeight(34)
        self.apply_button = QtWidgets.QPushButton("Apply settings")
        self.rounded_button_style = (
            "QPushButton, QToolButton { background-color: #303030; "
            "color: #eeeeee; border: 1px solid #555555; border-radius: 8px; "
            "padding: 5px 8px; }"
            "QPushButton:hover, QToolButton:hover { background-color: #3a3a3a; "
            "border-color: #888888; }"
            "QPushButton:pressed, QToolButton:pressed { background-color: #252525; }"
            "QPushButton:disabled, QToolButton:disabled { color: #888888; }"
            "QToolButton::menu-button { border-left: 1px solid #555555; "
            "border-top-right-radius: 7px; border-bottom-right-radius: 7px; "
            "width: 16px; }"
        )
        self.capture_once_button.setStyleSheet(self.rounded_button_style)
        self.apply_button.setStyleSheet(self.rounded_button_style)
        self.capture_once_button.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Expanding,
            QtWidgets.QSizePolicy.Policy.Fixed,
        )
        self.capture_once_button.setMinimumHeight(38)
        self.apply_button.setMinimumHeight(34)
        self.trigger_indicator = QtWidgets.QLabel("Trigger: idle")
        self.overflow_indicator = QtWidgets.QLabel("Overflow: none")
        self.trigger_indicator.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.overflow_indicator.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        for indicator in (self.trigger_indicator, self.overflow_indicator):
            indicator.setMinimumHeight(32)
            indicator.setStyleSheet(
                "QLabel { background: #242424; border: 1px solid #555555; "
                "border-radius: 8px; padding: 5px; }"
            )

        indicator_row = QtWidgets.QHBoxLayout()
        indicator_row.addWidget(self.trigger_indicator)
        indicator_row.addWidget(self.overflow_indicator)

        self.capture_rate_label = QtWidgets.QLabel("0.00 /s")
        self.last_capture_label = QtWidgets.QLabel("--")
        self.capture_count_label = QtWidgets.QLabel("0")
        self.capture_rate_card = self.create_readout_card(
            "CAPTURES", self.capture_rate_label
        )
        self.last_capture_card = self.create_readout_card(
            "LAST TCP", self.last_capture_label
        )
        self.capture_count_card = self.create_readout_card(
            "TOTAL", self.capture_count_label
        )
        capture_stats = QtWidgets.QGridLayout()
        capture_stats.setContentsMargins(0, 0, 0, 0)
        capture_stats.setSpacing(6)
        capture_stats.addWidget(self.capture_rate_card, 0, 0)
        capture_stats.addWidget(self.last_capture_card, 0, 1)
        capture_stats.addWidget(self.capture_count_card, 0, 2)
        for column in range(3):
            capture_stats.setColumnStretch(column, 1)
        controls = QtWidgets.QWidget()
        controls.setMinimumWidth(330)
        controls.setMaximumWidth(400)
        controls.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Preferred,
            QtWidgets.QSizePolicy.Policy.Preferred,
        )
        controls_layout = QtWidgets.QVBoxLayout(controls)
        controls_layout.setContentsMargins(8, 8, 8, 8)
        controls_layout.setSpacing(8)
        controls_layout.addWidget(acquisition_group)
        controls_layout.addWidget(trigger_group)
        controls_layout.addWidget(visualization_group)
        controls_layout.addWidget(self.capture_once_button)
        reset_menu = QtWidgets.QMenu(self)
        self.hard_reset_action = reset_menu.addAction("Hard reset (reboot Pico)")
        self.reset_button = QtWidgets.QToolButton()
        self.reset_button.setText("Soft reset")
        self.reset_button.setStyleSheet(self.rounded_button_style)
        self.reset_button.setToolButtonStyle(
            QtCore.Qt.ToolButtonStyle.ToolButtonTextOnly
        )
        self.reset_button.setPopupMode(
            QtWidgets.QToolButton.ToolButtonPopupMode.MenuButtonPopup
        )
        self.reset_button.setMenu(reset_menu)
        self.reset_button.setMinimumHeight(32)
        self.reset_button.setMinimumWidth(100)
        action_row = QtWidgets.QHBoxLayout()
        action_row.addWidget(self.apply_button, 1)
        action_row.addWidget(self.reset_button)
        controls_layout.addLayout(action_row)
        controls_layout.addWidget(voltage_card)
        controls_layout.addLayout(indicator_row)
        controls_layout.addLayout(capture_stats)
        controls_layout.addStretch(1)

        controls_scroll = QtWidgets.QScrollArea()
        controls_scroll.setWidgetResizable(True)
        controls_scroll.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        controls_scroll.setHorizontalScrollBarPolicy(
            QtCore.Qt.ScrollBarPolicy.ScrollBarAlwaysOff
        )
        controls_scroll.setVerticalScrollBarPolicy(
            QtCore.Qt.ScrollBarPolicy.ScrollBarAsNeeded
        )
        controls_scroll.setWidget(controls)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        splitter.addWidget(self.plot_stack)
        splitter.addWidget(controls_scroll)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 0)
        splitter.setSizes([800, 380])
        self.setCentralWidget(splitter)

        self.refresh_trigger_line()
        status_events.trigger_hit.connect(
            self.on_trigger_hit, QtCore.Qt.ConnectionType.QueuedConnection
        )
        status_events.overflow.connect(
            self.on_overflow, QtCore.Qt.ConnectionType.QueuedConnection
        )
        status_events.once_state.connect(
            self.on_once_state_changed, QtCore.Qt.ConnectionType.QueuedConnection
        )
        status_events.worker_error.connect(
            self.on_worker_error, QtCore.Qt.ConnectionType.QueuedConnection
        )
        status_events.device_seen.connect(
            self.on_device_seen, QtCore.Qt.ConnectionType.QueuedConnection
        )
        status_events.reset_complete.connect(
            self.on_reset_complete, QtCore.Qt.ConnectionType.QueuedConnection
        )
        voltage_events.reading.connect(
            self.on_voltage_reading, QtCore.Qt.ConnectionType.QueuedConnection
        )
        self.apply_button.clicked.connect(self.send_settings)
        self.capture_once_button.clicked.connect(self.capture_once_action)
        for widget in (
            self.trigger_voltage, self.edge, self.capture_length_ms,
            self.trigger_offset,
        ):
            if isinstance(widget, QtWidgets.QComboBox):
                widget.currentIndexChanged.connect(self.send_settings)
            else:
                widget.valueChanged.connect(self.send_settings)
        self.mode_group.buttonToggled.connect(
            lambda _button, checked: self.send_settings() if checked else None
        )
        self.trigger_repeat_group.buttonToggled.connect(
            lambda _button, checked: self.send_settings() if checked else None
        )
        self.sample_rate.activated.connect(self.select_sample_rate)
        self.plot_view.currentIndexChanged.connect(self.refresh_plot_view)
        self.client_average.currentIndexChanged.connect(
            self.refresh_client_average
        )
        self.trigger_voltage.valueChanged.connect(self.refresh_trigger_line)
        self.mode_group.buttonToggled.connect(self.refresh_trigger_line)
        self.mode_group.buttonToggled.connect(self.refresh_once_button)
        self.trigger_repeat_group.buttonToggled.connect(self.refresh_once_button)
        self.capture_length_ms.editingFinished.connect(
            self.normalize_capture_length
        )
        self.trigger_line.sigPositionChanged.connect(
            self.sync_trigger_control_from_line
        )
        self.trigger_line.sigPositionChangeFinished.connect(
            self.commit_trigger_line
        )

        self.status = self.statusBar()
        self.status.setStyleSheet("font-size: 13px; font-weight: bold")
        self.status.showMessage("Benchmarking...")
        self.device_indicator = QtWidgets.QLabel()
        self.device_indicator.setFixedSize(12, 12)
        self.device_indicator.setStyleSheet(
            "background-color: #777777; border-radius: 6px;"
        )
        self.device_status_label = QtWidgets.QLabel("Pico disconnected")
        self.device_status_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        device_status = QtWidgets.QWidget()
        device_status_layout = QtWidgets.QHBoxLayout(device_status)
        device_status_layout.setContentsMargins(4, 0, 8, 0)
        device_status_layout.setSpacing(6)
        device_status_layout.addWidget(self.device_indicator)
        device_status_layout.addWidget(self.device_status_label)
        self.status.addWidget(device_status)
        self.last_device_packet_time = None
        self.device_connected = False
        self.device_timer = QtCore.QTimer(self)
        self.device_timer.timeout.connect(self.check_device_connection)
        self.device_timer.start(250)

        self.last_time = time.perf_counter()
        self.last_frames = 0
        self.last_bytes = 0
        self.last_capture_stat_time = self.last_time
        self.last_capture_stat_count = 0

        self.plot_timer = QtCore.QTimer()
        self.plot_timer.timeout.connect(self.update_plot)
        self.plot_timer.start(1000 // 60)
        self.trigger_flash_timer = QtCore.QTimer(self)
        self.trigger_flash_timer.setSingleShot(True)
        self.trigger_flash_timer.timeout.connect(self.reset_trigger_indicator)
        self.overflow_flash_timer = QtCore.QTimer(self)
        self.overflow_flash_timer.setSingleShot(True)
        self.overflow_flash_timer.timeout.connect(self.reset_overflow_indicator)

        self.bench_timer = QtCore.QTimer()
        self.bench_timer.timeout.connect(self.update_benchmark)
        self.bench_timer.start(1000)
        self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.last_control_address = None
        self.reset_button.clicked.connect(self.soft_reset_scope)
        self.hard_reset_action.triggered.connect(self.hard_reset_scope)
        self.send_settings()

        for label in self.findChildren(QtWidgets.QLabel):
            if label is not self.device_status_label:
                label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

    @staticmethod
    def create_choice_buttons(labels):
        container = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)
        group = QtWidgets.QButtonGroup(container)
        group.setExclusive(True)
        buttons = []
        for index, text in enumerate(labels):
            button = QtWidgets.QPushButton(text)
            button.setCheckable(True)
            button.setMinimumHeight(30)
            button.setStyleSheet(
                "QPushButton { padding: 4px; border: 1px solid #555555; "
                "border-radius: 8px; }"
                "QPushButton:hover { border-color: #888888; }"
                "QPushButton:checked { background-color: #315b3a; "
                "border-color: #5d9b6a; }"
            )
            group.addButton(button, index)
            layout.addWidget(button)
            buttons.append(button)
        return container, group, buttons

    @staticmethod
    def create_readout_card(title, value_label):
        card = QtWidgets.QFrame()
        card.setStyleSheet(READOUT_CARD_STYLE)
        layout = QtWidgets.QVBoxLayout(card)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setSpacing(2)
        title_label = QtWidgets.QLabel(title)
        title_label.setStyleSheet(
            "color: #aaaaaa; font-size: 8pt; border: none;"
        )
        value_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        value_label.setStyleSheet(
            "color: #eeeeee; font-size: 10pt; font-weight: bold; border: none;"
        )
        layout.addWidget(title_label)
        layout.addWidget(value_label)
        return card

    @staticmethod
    def compact_count(value):
        if value >= 1_000_000:
            return f"{value / 1_000_000:.2f}M"
        if value >= 10_000:
            return f"{value / 1_000:.1f}k"
        return f"{value:,}"

    def reset_trigger_indicator(self):
        self.trigger_indicator.setText("Trigger: idle")
        self.trigger_indicator.setStyleSheet(
            "QLabel { background: #242424; color: #eeeeee; border: 1px solid "
            "#555555; border-radius: 8px; padding: 5px; }"
        )

    def reset_overflow_indicator(self):
        if self.last_capture_overflows:
            self.overflow_indicator.setText(
                f"Overflow: {self.last_capture_overflows} buffer(s)"
            )
        else:
            self.overflow_indicator.setText("Overflow: none")
        self.overflow_indicator.setStyleSheet(
            "QLabel { background: #242424; color: #eeeeee; border: 1px solid "
            "#555555; border-radius: 8px; padding: 5px; }"
        )

    def on_trigger_hit(self, _value):
        now = time.monotonic()
        if now - self.last_trigger_flash_time < STATUS_FLASH_MIN_INTERVAL:
            return
        self.last_trigger_flash_time = now
        self.trigger_indicator.setText("Trigger: hit")
        self.trigger_indicator.setStyleSheet(
            "QLabel { background: #fff2b3; color: #202020; border: 1px solid "
            "#d9b84c; border-radius: 8px; padding: 5px; }"
        )
        self.trigger_flash_timer.start(STATUS_FLASH_DURATION_MS)

    def on_overflow(self, value):
        self.last_capture_overflows = max(self.last_capture_overflows, value)
        self.overflow_indicator.setText(f"Overflow: {value} buffer(s)")
        now = time.monotonic()
        if now - self.last_overflow_flash_time >= STATUS_FLASH_MIN_INTERVAL:
            self.last_overflow_flash_time = now
            self.overflow_indicator.setStyleSheet(
                "QLabel { background: #ffb0b0; color: #202020; border: 1px solid "
                "#d97979; border-radius: 8px; padding: 5px; }"
            )
            self.overflow_flash_timer.start(STATUS_FLASH_DURATION_MS)

    def on_voltage_reading(self, average, sample_count):
        voltage = average * VSCALE / 4095.0
        self.live_voltage_label.setText(f"{voltage:.3f} V")
        self.live_voltage_detail.setText(
            f"ADC {average} / 4095  |  avg {sample_count}"
        )

    def on_device_seen(self, address):
        global viewer_address
        viewer_address = address
        self.last_device_packet_time = time.monotonic()
        self.set_device_connected(True)
        if address != self.last_control_address:
            self.send_settings()

    def set_device_connected(self, connected):
        self.device_connected = connected
        if connected:
            color = "#39d353"
            text = "Pico connected"
        else:
            color = "#777777"
            text = "Pico disconnected"
        self.device_indicator.setStyleSheet(
            f"background-color: {color}; border-radius: 6px;"
        )
        self.device_status_label.setText(text)
        self.device_indicator.setToolTip(text)

    def on_worker_error(self, message):
        self.status.showMessage(message, 10000)

    def on_reset_complete(self):
        with frame_queue.mutex:
            frame_queue.queue.clear()
        with capture_queue.mutex:
            capture_queue.queue.clear()
        self.last_raw_capture = None
        self.last_capture_length = 0
        self.last_capture_overflows = 0
        self.last_capture_seconds = 0.0
        self.capture_count = 0
        self.capture_rate = 0.0
        self.last_capture_stat_count = 0
        self.last_capture_stat_time = time.monotonic()
        self.capture_count_label.setText("0")
        self.capture_rate_label.setText("0.00 /s")
        self.last_capture_label.setText("--")

        for group in (self.mode_group, self.trigger_repeat_group):
            group.blockSignals(True)
        for widget in (
            self.sample_rate, self.trigger_voltage, self.edge,
            self.capture_length_ms, self.trigger_offset,
        ):
            widget.blockSignals(True)
        self.mode_group.button(0).setChecked(True)
        self.trigger_repeat_group.button(0).setChecked(True)
        self.sample_rate.setCurrentIndex(
            self.sample_rate.findData(NOMINAL_ADC_SPS)
        )
        self.selected_sample_rate = NOMINAL_ADC_SPS
        self.refresh_sample_rate()
        self.trigger_voltage.setValue(VSCALE / 2)
        self.edge.setCurrentIndex(0)
        self.trigger_offset.setValue(50)
        self.capture_length_ms.setValue(
            MAX_CAPTURE_SAMPLES / NOMINAL_ADC_SPS * 1000
        )
        for widget in (
            self.sample_rate, self.trigger_voltage, self.edge,
            self.capture_length_ms, self.trigger_offset,
        ):
            widget.blockSignals(False)
        for group in (self.mode_group, self.trigger_repeat_group):
            group.blockSignals(False)
        self.history_samples = max(
            1, HISTORY_SAMPLES // self.client_average_factor()
        )
        duration = HISTORY_SAMPLES / NOMINAL_ADC_SPS
        self.history = np.zeros(self.history_samples, dtype=np.uint16)
        self.x_data = np.linspace(-duration, 0.0, self.history_samples)
        self.history_sample_rate = (
            NOMINAL_ADC_SPS / self.client_average_factor()
        )
        if self.plot_view.currentIndex() == 0:
            self.plot_widget.setXRange(-duration, 0.0)
        self.refresh_trigger_line()
        self.refresh_once_button()
        self.reset_overflow_indicator()
        self.curve.setData(self.x_data, self.history)
        if self.plot_view.currentIndex() == 1:
            self.update_spectrum()
        self.status.showMessage("Acquisition restarted", 5000)

    def on_once_state_changed(self, armed):
        if not self.trigger_once_selected():
            return
        state = "ARMED" if armed else "DISARMED"
        self.capture_once_button.setText(f"Trigger arm · {state}")
        self.capture_once_button.setStyleSheet(
            self.rounded_button_style
            + "QPushButton { background-color: #315b3a; "
            "border-color: #5d9b6a; }"
            if armed else
            self.rounded_button_style
            + "QPushButton { background-color: #663b32; "
            "border-color: #a35d50; }"
        )

    def trigger_once_selected(self):
        return (
            self.mode_index() == 1
            and self.trigger_repeat_index() == 0
        )

    def refresh_once_button(self, *_):
        if self.trigger_once_selected():
            self.capture_once_button.setText("Trigger arm · WAIT")
        else:
            self.capture_once_button.setText("Trigger arm")
        self.capture_once_button.setStyleSheet(self.rounded_button_style)

    def capture_once_action(self):
        if self.trigger_once_selected():
            self.capture_once_button.setText("Trigger arm · ARMING")
            self.send_settings()
        else:
            self.send_settings(mode_override=1)

    def refresh_trigger_line(self, *_):
        adc_level = self.trigger_voltage.value() / VSCALE * 4096
        self.trigger_line.setValue(adc_level)
        self.trigger_line.setVisible(self.mode_index() == 1)
        self.trigger_line.setMovable(self.mode_index() == 1)

    def sync_trigger_control_from_line(self, *_):
        voltage = self.trigger_line.value() / 4096 * VSCALE
        signals_blocked = self.trigger_voltage.blockSignals(True)
        try:
            self.trigger_voltage.setValue(voltage)
        finally:
            self.trigger_voltage.blockSignals(signals_blocked)

    def commit_trigger_line(self, *_):
        self.refresh_trigger_line()
        self.send_settings()

    def refresh_plot_view(self, index):
        self.plot_stack.setCurrentIndex(index)
        if index == 1:
            self.update_spectrum()

    def update_spectrum(self):
        sample_count = len(self.history)
        if sample_count < 4 or self.history_sample_rate <= 0:
            return

        window = np.hanning(sample_count)
        window_sum = window.sum()
        if window_sum == 0:
            return

        centered = self.history.astype(np.float64)
        centered -= centered.mean()
        spectrum = np.abs(np.fft.rfft(centered * window))
        spectrum *= 2.0 / window_sum
        if sample_count % 2 == 0:
            spectrum[-1] *= 0.5
        spectrum *= VSCALE / 4096.0
        spectrum[0] = 0.0
        frequencies = np.fft.rfftfreq(
            sample_count, d=1.0 / self.history_sample_rate
        )

        self.spectrum_curve.setData(frequencies, spectrum)

    def check_device_connection(self):
        address = viewer_address
        if address is not None and address != self.last_control_address:
            self.last_control_address = address
            self.send_settings()
        connected = (
            self.last_device_packet_time is not None
            and time.monotonic() - self.last_device_packet_time < 1.5
        )
        if connected != self.device_connected:
            self.set_device_connected(connected)

    def mode_index(self):
        return self.mode_group.checkedId()

    def trigger_repeat_index(self):
        return self.trigger_repeat_group.checkedId()

    def soft_reset_scope(self, *_):
        if viewer_address is None:
            self.status.showMessage("Waiting for Pico connection...")
            return
        try:
            self.control_socket.sendto(b"OSCR", (viewer_address, CONTROL_PORT))
            self.status.showMessage("Restarting acquisition...", 5000)
        except OSError as error:
            self.status.showMessage(f"Could not reset oscilloscope: {error}", 10000)

    def hard_reset_scope(self, *_):
        if viewer_address is None:
            self.status.showMessage("Waiting for Pico connection...")
            return
        try:
            self.control_socket.sendto(b"OSCH", (viewer_address, CONTROL_PORT))
            self.status.showMessage("Hard reset: Pico rebooting", 5000)
            self.last_device_packet_time = None
            self.set_device_connected(False)
        except OSError as error:
            self.status.showMessage(f"Could not reboot oscilloscope: {error}", 10000)

    def refresh_sample_rate(self, *_):
        sample_rate = self.sample_rate.currentData()
        if sample_rate is None:
            return
        duration = HISTORY_SAMPLES / sample_rate
        self.x_data = np.linspace(-duration, 0.0, self.history_samples)
        if self.last_raw_capture is None:
            self.history_sample_rate = (
                sample_rate / self.client_average_factor()
            )
        if not self.last_raw_capture and self.plot_view.currentIndex() == 0:
            self.plot_widget.setXRange(-duration, 0.0)
        elif self.last_raw_capture is None and self.plot_view.currentIndex() == 1:
            self.update_spectrum()
        self.capture_length_ms.setRange(
            SAMPLE_BUFFER_SIZE / sample_rate * 1000,
            MAX_CAPTURE_SAMPLES / sample_rate * 1000,
        )
        self.capture_length_ms.setSingleStep(
            SAMPLE_BUFFER_SIZE / sample_rate * 1000
        )
        self.normalize_capture_length()

    def client_average_factor(self):
        return self.client_average.currentData()

    def average_for_display(self, samples):
        factor = self.client_average_factor()
        usable_count = len(samples) - len(samples) % factor
        if usable_count == 0:
            return np.empty(0, dtype=np.uint16)
        if factor == 1:
            return samples[:usable_count]
        grouped = samples[:usable_count].reshape(-1, factor)
        return np.rint(grouped.mean(axis=1)).astype(np.uint16)

    def refresh_client_average(self, *_):
        factor = self.client_average_factor()
        self.history_samples = max(1, HISTORY_SAMPLES // factor)
        if self.last_raw_capture is not None:
            samples, buffer_capacity, overflows, trigger_index, sample_rate, \
                transfer_seconds, payload_length = self.last_raw_capture
            self.display_capture(
                samples, buffer_capacity, overflows, trigger_index, sample_rate,
                transfer_seconds, payload_length, new_capture=False,
            )
            return
        self.history = np.zeros(self.history_samples, dtype=np.uint16)
        duration = HISTORY_SAMPLES / self.sample_rate_value()
        self.x_data = np.linspace(-duration, 0.0, self.history_samples)
        self.history_sample_rate = (
            self.sample_rate_value() / factor
        )
        if self.plot_view.currentIndex() == 0:
            self.plot_widget.setXRange(-duration, 0.0)
        self.curve.setData(self.x_data, self.history)
        if self.plot_view.currentIndex() == 1:
            self.update_spectrum()

    def capture_length_samples(self):
        requested_samples = round(
            self.capture_length_ms.value() * self.sample_rate_value() / 1000
        )
        capture_length = min(
            MAX_CAPTURE_SAMPLES,
            max(SAMPLE_BUFFER_SIZE, requested_samples),
        )
        return (
            (capture_length + SAMPLE_BUFFER_SIZE - 1) // SAMPLE_BUFFER_SIZE
        ) * SAMPLE_BUFFER_SIZE

    def normalize_capture_length(self):
        sample_rate = self.sample_rate_value()
        actual_duration_ms = (
            self.capture_length_samples() / sample_rate * 1000
        )
        if abs(self.capture_length_ms.value() - actual_duration_ms) > 1e-6:
            self.capture_length_ms.setValue(actual_duration_ms)
            self.send_settings()

    def select_sample_rate(self, index):
        rate = self.sample_rate.itemData(index)
        if rate is None:
            previous_index = self.sample_rate.findData(self.selected_sample_rate)
            custom_rate, accepted = QtWidgets.QInputDialog.getInt(
                self,
                "Custom sample rate",
                f"Sample rate ({MIN_ADC_SPS:,}–{NOMINAL_ADC_SPS:,} samples/s):",
                self.selected_sample_rate,
                MIN_ADC_SPS,
                NOMINAL_ADC_SPS,
            )
            if not accepted:
                self.sample_rate.setCurrentIndex(previous_index)
                return
            rate = custom_rate
            custom_index = self.sample_rate.findData(rate)
            if custom_index == -1:
                custom_index = self.sample_rate.count() - 1
                self.sample_rate.insertItem(
                    custom_index, f"{rate:,} S/s (custom)", rate
                )
            self.sample_rate.setCurrentIndex(custom_index)

        self.selected_sample_rate = rate
        self.refresh_sample_rate()
        self.send_settings()

    def sample_rate_value(self):
        rate = self.sample_rate.currentData()
        return rate if rate is not None else self.selected_sample_rate

    def send_settings(self, *_, mode_override=None):
        if viewer_address is None:
            self.status.showMessage("Waiting for oscilloscope UDP samples...")
            return

        mode = mode_override
        if mode is None:
            mode = 2 if self.mode_index() == 1 else 0
        capture_length = self.capture_length_samples()
        message = (
            f"{mode} {self.sample_rate_value()} "
            f"{self.trigger_voltage.value():.4f} {self.edge.currentIndex()} "
            f"{self.trigger_repeat_index()} {capture_length} "
            f"{self.trigger_offset.value()}"
        )
        try:
            self.control_socket.sendto(
                message.encode("ascii"), (viewer_address, CONTROL_PORT)
            )
            self.last_control_address = viewer_address
        except OSError as error:
            self.status.showMessage(f"Could not send settings: {error}")

    def display_capture(self, samples, buffer_capacity, overflows, trigger_index,
                        sample_rate, transfer_seconds, payload_length,
                        new_capture=True):
        sample_rate = sample_rate or self.sample_rate_value()
        factor = self.client_average_factor()
        averaged = self.average_for_display(samples)
        self.last_capture_length = len(samples)
        self.last_capture_overflows = overflows
        self.last_capture_seconds = transfer_seconds
        self.history = averaged
        self.history_sample_rate = sample_rate / factor
        displayed_trigger_index = (
            (trigger_index - (factor - 1) / 2) / factor
            if trigger_index != 0xFFFFFFFF else None
        )
        if displayed_trigger_index is None:
            self.x_data = (
                np.arange(len(averaged)) * factor + (factor - 1) / 2
            ) / sample_rate
            self.plot_widget.setLabel("bottom", "Sample time", units="s")
        else:
            self.x_data = (
                np.arange(len(averaged)) * factor + (factor - 1) / 2 -
                trigger_index
            ) / sample_rate
            self.plot_widget.setLabel("bottom", "Time from trigger", units="s")
        self.plot_widget.setXRange(self.x_data[0], self.x_data[-1])
        self.curve.setData(self.x_data, self.history)
        if self.plot_view.currentIndex() == 1:
            self.update_spectrum()

        self.reset_overflow_indicator()
        if new_capture:
            self.capture_count += 1
        self.last_capture_label.setText(f"{transfer_seconds:.2f} s")
        self.capture_count_label.setText(self.compact_count(self.capture_count))

    def update_plot(self):
        capture_updated = False
        while not capture_queue.empty():
            (samples, buffer_capacity, overflows, trigger_index, sample_rate,
             transfer_seconds, payload_length, error) = capture_queue.get_nowait()
            if error is not None:
                self.status.showMessage(f"Capture error: {error}", 5000)
            elif samples is None:
                if self.last_capture_length:
                    self.last_capture_overflows = max(
                        self.last_capture_overflows, overflows
                    )
            else:
                sample_rate = sample_rate or self.sample_rate_value()
                max_capture_ms = (
                    buffer_capacity / self.sample_rate_value() * 1000
                )
                self.capture_length_ms.setMaximum(max_capture_ms)
                if self.capture_length_ms.value() > max_capture_ms:
                    self.capture_length_ms.setValue(max_capture_ms)
                self.last_raw_capture = (
                    samples, buffer_capacity, overflows, trigger_index,
                    sample_rate, transfer_seconds, payload_length,
                )
                self.display_capture(
                    samples, buffer_capacity, overflows, trigger_index,
                    sample_rate, transfer_seconds, payload_length,
                )
                capture_updated = True
        if capture_updated:
            return

        frames = []
        while not frame_queue.empty():
            frames.append(frame_queue.get_nowait())

        if not frames:
            return

        new_data = np.concatenate(frames)
        new_data = self.average_for_display(new_data)
        n = len(new_data)
        self.last_raw_capture = None
        self.history_sample_rate = (
            self.sample_rate_value() / self.client_average_factor()
        )

        if len(self.history) != self.history_samples:
            self.history = np.zeros(self.history_samples, dtype=np.uint16)
            duration = HISTORY_SAMPLES / self.sample_rate_value()
            self.x_data = np.linspace(-duration, 0.0, self.history_samples)
            self.plot_widget.setXRange(-duration, 0.0)
        if n >= self.history_samples:
            self.history[:] = new_data[-self.history_samples:]
        else:
            self.history = np.roll(self.history, -n)
            self.history[-n:] = new_data

        self.curve.setData(self.x_data, self.history)
        if self.plot_view.currentIndex() == 1:
            self.update_spectrum()

    def update_benchmark(self):
        now = time.perf_counter()
        dt = now - self.last_time

        with stats_lock:
            current_frames = rx_frame_count
            current_bytes = rx_byte_count

        d_frames = current_frames - self.last_frames
        d_bytes = current_bytes - self.last_bytes

        self.last_time = now
        self.last_frames = current_frames
        self.last_bytes = current_bytes

        if dt > 0:
            fps = d_frames / dt
            effective_sps = fps * TOTAL_SAMPLES
            effective_ksps = effective_sps / 1000.0
            nominal_ksps = self.sample_rate_value() / 1000.0

            duty_cycle = (effective_sps / self.sample_rate_value()) * 100.0
            mbps = (d_bytes * 8) / (dt * 1e6)

            msg = (
                f"Sample Speed: {duty_cycle:.2f}% of {nominal_ksps:.0f} kSps  |  "
                f"Rate: {effective_ksps:.1f} kS/s ({fps:.1f} FPS)  |  "
                f"Network: {mbps:.2f} Mbps"
            )

            self.status.showMessage(msg)

        capture_elapsed = now - self.last_capture_stat_time
        if capture_elapsed >= 1.0:
            captures = self.capture_count - self.last_capture_stat_count
            self.capture_rate = captures / capture_elapsed
            self.capture_rate_label.setText(f"Captures/s: {self.capture_rate:.2f}")
            self.last_capture_stat_time = now
            self.last_capture_stat_count = self.capture_count

    def closeEvent(self, event):
        self.plot_timer.stop()
        self.bench_timer.stop()
        self.device_timer.stop()
        self.trigger_flash_timer.stop()
        self.overflow_flash_timer.stop()
        stop_workers()
        self.control_socket.close()
        event.accept()

if __name__ == "__main__":
    app = QtWidgets.QApplication([])
    window = LivePlot()
    worker_stop.clear()
    with frame_queue.mutex:
        frame_queue.queue.clear()
    with capture_queue.mutex:
        capture_queue.queue.clear()
    worker_threads = [
        threading.Thread(target=udp_worker, name="sample-udp", daemon=True),
        threading.Thread(target=tcp_capture_worker, name="capture-tcp", daemon=True),
        threading.Thread(target=status_worker, name="status-udp", daemon=True),
        threading.Thread(target=voltage_worker, name="voltage-udp", daemon=True),
    ]
    for thread in worker_threads:
        thread.start()
    window.show()
    try:
        app.exec()
    finally:
        stop_workers()
        window.control_socket.close()
