import socket
import struct
import threading
import queue
import time
import contextlib
from collections import deque
import numpy as np
import pyqtgraph as pg
from scipy import signal
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
UDP_FRAME_EXPIRY_SEC = 0.25
NUM_RING_BUFFERS = 84
SAMPLE_BUFFER_SIZE = 1024
MAX_CAPTURE_SAMPLES = NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE

NOMINAL_ADC_SPS = 500_000
MIN_ADC_SPS = 733
# HISTORY_SEC = 0.1
HISTORY_SEC = (TOTAL_SAMPLES*2) / NOMINAL_ADC_SPS
HISTORY_SAMPLES = int(HISTORY_SEC * NOMINAL_ADC_SPS)
Y_AXIS_DIVISIONS = 8
AUTOSCALE_WINDOW_SECONDS = 1.0
AUTOSCALE_SMOOTHING_SECONDS = 1.0

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
rx_packet_count = 0
rx_incomplete_count = 0
device_tx_packet_count = 0
device_tx_error_count = 0
device_stream_block_count = 0
device_dma_drop_count = 0
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

class ScopeViewBox(pg.ViewBox):
    user_y_interaction = QtCore.pyqtSignal()

    def wheelEvent(self, event, axis=None):
        super().wheelEvent(event, axis=0)

    def mouseDragEvent(self, event, axis=None):
        if self.state["mouseEnabled"][1]:
            self.user_y_interaction.emit()
        super().mouseDragEvent(event, axis=axis)

class ScopeAxisItem(pg.AxisItem):
    wheel_zoom = QtCore.pyqtSignal(int)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wheel_delta = 0

    def wheelEvent(self, event):
        delta = event.delta()
        if delta:
            self.wheel_delta += delta
            while self.wheel_delta >= 240:
                self.wheel_zoom.emit(1)
                self.wheel_delta -= 240
            while self.wheel_delta <= -240:
                self.wheel_zoom.emit(-1)
                self.wheel_delta += 240
            event.accept()
        else:
            event.ignore()

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
    global rx_frame_count, rx_byte_count, rx_packet_count, rx_incomplete_count
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", PORT))
        sock.settimeout(0.5)
        register_worker_socket(sock)
        frames = {}
        frame_first_seen = {}

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
            received_at = time.monotonic()
            data_offset = len(packet) - (SAMPLES_PER_PACKET * 2)
            samples = np.frombuffer(
                packet, dtype=np.uint16, offset=data_offset,
                count=SAMPLES_PER_PACKET
            )

            expired_frames = [
                count for count, first_seen in frame_first_seen.items()
                if received_at - first_seen > UDP_FRAME_EXPIRY_SEC
            ]
            if sample_count not in frames and len(frames) >= 20:
                expired_frames.append(min(frames))
            if expired_frames:
                expired_frames = set(expired_frames)
                with stats_lock:
                    rx_incomplete_count += len(expired_frames)
                for count in expired_frames:
                    frames.pop(count, None)
                    frame_first_seen.pop(count, None)

            with stats_lock:
                rx_packet_count += 1
                rx_byte_count += len(packet)
            if sample_count not in frames:
                frames[sample_count] = {}
                frame_first_seen[sample_count] = received_at
            frames[sample_count][sample_part] = samples

            if len(frames[sample_count]) == NUM_PARTS:
                full_frame = np.concatenate(
                    [frames[sample_count][part] for part in range(NUM_PARTS)]
                )
                del frames[sample_count]
                frame_first_seen.pop(sample_count, None)
                with stats_lock:
                    rx_frame_count += 1
                if frame_queue.full():
                    with contextlib.suppress(queue.Empty):
                        frame_queue.get_nowait()
                frame_queue.put(full_frame)
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
    global device_tx_packet_count, device_tx_error_count
    global device_stream_block_count, device_dma_drop_count
    if len(packet) != 9 or packet[:4] != b"OSCE":
        return False
    event, value = struct.unpack_from("<BI", packet, 4)
    if event == 1:
        status_events.trigger_hit.emit(value)
    elif event == 2:
        with stats_lock:
            device_dma_drop_count = value
        status_events.overflow.emit(value)
    elif event == 3:
        status_events.once_state.emit(bool(value))
    elif event == 4:
        with stats_lock:
            device_dma_drop_count = 0
        status_events.reset_complete.emit()
    elif event == 5:
        with stats_lock:
            device_tx_packet_count = value
    elif event == 6:
        with stats_lock:
            device_tx_error_count = value
    elif event == 7:
        with stats_lock:
            device_stream_block_count = value
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
        self.live_effective_sample_rate = NOMINAL_ADC_SPS
        self.filter_specs = ()
        self.filter_sos = None
        self.live_filter_state = None
        self.filter_text = ""
        self.filter_editor = None
        self.filter_dialog = None
        self.filter_dialog_error = None
        self.last_raw_capture = None
        self.capture_view_initialized = False
        self.snapped_capture_length_samples = None
        self.normalizing_capture_length = False

        self.y_axis = ScopeAxisItem("left")
        self.scope_view_box = ScopeViewBox()
        self.plot_widget = pg.PlotWidget(
            viewBox=self.scope_view_box,
            axisItems={"left": self.y_axis},
        )
        self.plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self.plot_widget.setYRange(0, 4096)
        self.plot_widget.setXRange(-HISTORY_SEC, 0.0)
        self.plot_widget.getViewBox().setMouseEnabled(x=True, y=True)
        self.plot_widget.setToolTip(
            "Drag and use the mouse wheel over the plot to pan/zoom time. "
            "Scroll over the Y-axis labels to change volts per division."
        )
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
        self.spectrum_peaks = self.spectrum_widget.plot(
            [], [], pen=None, symbol="o", symbolSize=8,
            symbolBrush="#ffcc66", symbolPen=pg.mkPen("#ffffff", width=1),
        )
        self.spectrum_peak_labels = []
        for _ in range(5):
            label = pg.TextItem(
                color="#ffcc66", anchor=(0.5, 1.1), fill=(20, 20, 20, 180)
            )
            label.setZValue(10)
            label.hide()
            self.spectrum_widget.addItem(label)
            self.spectrum_peak_labels.append(label)
        self.spectrum_widget.getViewBox().sigRangeChanged.connect(
            self.keep_spectrum_y_zero
        )
        self.plot_stack = QtWidgets.QStackedWidget()
        self.time_view = QtWidgets.QWidget()
        time_view_layout = QtWidgets.QVBoxLayout(self.time_view)
        time_view_layout.setContentsMargins(0, 0, 0, 0)
        time_view_layout.setSpacing(4)
        axis_controls = QtWidgets.QHBoxLayout()
        axis_controls.setContentsMargins(6, 4, 6, 0)
        axis_controls.setSpacing(6)
        axis_controls.addWidget(QtWidgets.QLabel("Y"))
        self.y_scale = QtWidgets.QComboBox()
        self.y_scale_steps = (
            0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0,
        )
        for volts_per_division in self.y_scale_steps:
            label = (
                f"{volts_per_division * 1000:g} mV/div"
                if volts_per_division < 1
                else f"{volts_per_division:g} V/div"
            )
            self.y_scale.addItem(label, volts_per_division)
        self.y_scale.setCurrentIndex(self.y_scale.findData(1.0))
        self.y_scale.setToolTip(
            "Choose a standard vertical scale, or scroll over the Y-axis labels."
        )
        self.y_scale.setStyleSheet("QComboBox { max-width: 125px; }")
        axis_controls.addWidget(self.y_scale)
        axis_controls.addWidget(QtWidgets.QLabel("Offset"))
        self.y_position = QtWidgets.QDoubleSpinBox()
        self.y_position.setRange(-VSCALE, 2 * VSCALE)
        self.y_position.setDecimals(2)
        self.y_position.setSingleStep(0.1)
        self.y_position.setValue(VSCALE / 2)
        self.y_position.setSuffix(" V")
        self.y_position.setToolTip(
            "Vertical center voltage. You can also drag vertically in the plot."
        )
        self.y_position.setStyleSheet(
            "QDoubleSpinBox { max-width: 110px; }"
        )
        axis_controls.addWidget(self.y_position)
        self.auto_y_button = QtWidgets.QPushButton("Auto Y")
        self.auto_y_button.setCheckable(True)
        self.auto_y_button.setToolTip(
            "Fit the recent one-second signal envelope, then smoothly follow "
            "changes over about one second."
        )
        self.auto_y_button.setStyleSheet(
            "QPushButton { background-color: #303030; color: #eeeeee; "
            "border: 1px solid #555555; border-radius: 8px; padding: 4px 10px; }"
            "QPushButton:checked { background-color: #315b3a; "
            "border-color: #5d9b6a; }"
        )
        axis_controls.addWidget(self.auto_y_button)
        axis_controls.addStretch(1)
        time_view_layout.addLayout(axis_controls)
        time_view_layout.addWidget(self.plot_widget, 1)
        self.plot_stack.addWidget(self.time_view)
        self.plot_stack.addWidget(self.spectrum_widget)
        self.autoscale_envelope = deque()
        self.autoscale_last_update = None

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
        self.capture_length_ms.setDecimals(1)
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
        initial_capture_samples = MAX_CAPTURE_SAMPLES
        self.active_settings_mode = self.mode_group.checkedId()
        self.mode_settings = {
            0: {
                "sample_rate": NOMINAL_ADC_SPS,
                "capture_samples": initial_capture_samples,
            },
            1: {
                "sample_rate": NOMINAL_ADC_SPS,
                "capture_samples": initial_capture_samples,
            },
        }
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
        filter_controls = QtWidgets.QWidget()
        filter_actions = QtWidgets.QHBoxLayout(filter_controls)
        filter_actions.setContentsMargins(0, 0, 0, 0)
        self.filter_status_label = QtWidgets.QLabel("No client filters active")
        self.filter_status_label.setWordWrap(True)
        self.configure_filters_button = QtWidgets.QPushButton(
            "Configure filters…"
        )
        self.filters_enabled_checkbox = QtWidgets.QCheckBox("On")
        self.filters_enabled_checkbox.setToolTip(
            "Quickly enable or bypass the configured client-side filters."
        )
        filter_actions.addWidget(self.filter_status_label, 1)
        filter_actions.addWidget(self.filters_enabled_checkbox)
        filter_actions.addWidget(self.configure_filters_button)
        visualization_form.addRow("Client filters", filter_controls)

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
            self.trigger_voltage, self.edge, self.trigger_offset,
        ):
            if isinstance(widget, QtWidgets.QComboBox):
                widget.currentIndexChanged.connect(self.send_settings)
            else:
                widget.valueChanged.connect(self.send_settings)
        self.capture_length_ms.valueChanged.connect(
            self.on_capture_length_changed
        )
        self.mode_group.buttonToggled.connect(self.on_mode_changed)
        self.trigger_repeat_group.buttonToggled.connect(
            lambda _button, checked: self.send_settings() if checked else None
        )
        self.sample_rate.activated.connect(self.select_sample_rate)
        self.plot_view.currentIndexChanged.connect(self.refresh_plot_view)
        self.client_average.currentIndexChanged.connect(
            self.refresh_client_average
        )
        self.configure_filters_button.clicked.connect(
            self.open_filter_dialog
        )
        self.filters_enabled_checkbox.toggled.connect(
            self.on_filters_enabled_changed
        )
        self.y_scale.currentIndexChanged.connect(self.on_manual_y_change)
        self.y_position.valueChanged.connect(self.on_manual_y_change)
        self.scope_view_box.user_y_interaction.connect(
            lambda: self.auto_y_button.setChecked(False)
        )
        self.y_axis.wheel_zoom.connect(self.zoom_y_axis)
        self.plot_widget.getViewBox().sigRangeChanged.connect(
            self.on_plot_range_changed
        )
        self.auto_y_button.toggled.connect(self.on_autoscale_toggled)
        self.trigger_voltage.valueChanged.connect(self.refresh_trigger_line)
        self.mode_group.buttonToggled.connect(self.refresh_trigger_line)
        self.mode_group.buttonToggled.connect(self.refresh_once_button)
        self.trigger_repeat_group.buttonToggled.connect(self.refresh_once_button)
        self.capture_length_ms.valueChanged.connect(
            self.refresh_acquisition_time_window
        )
        self.capture_length_ms.editingFinished.connect(
            self.normalize_capture_length
        )
        self.trigger_offset.valueChanged.connect(
            self.refresh_acquisition_time_window
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
        self.last_rx_packets = 0
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
        self.apply_y_axis_range()
        self.refresh_acquisition_time_window()
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
        self.live_filter_state = None
        self.last_raw_capture = None
        self.capture_view_initialized = False
        self.autoscale_envelope.clear()
        self.autoscale_last_update = time.monotonic()
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
        self.active_settings_mode = 0
        self.mode_settings = {
            0: {
                "sample_rate": NOMINAL_ADC_SPS,
                "capture_samples": MAX_CAPTURE_SAMPLES,
            },
            1: {
                "sample_rate": NOMINAL_ADC_SPS,
                "capture_samples": MAX_CAPTURE_SAMPLES,
            },
        }
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
        capture_samples = self.capture_length_samples()
        self.history_samples = max(
            1, capture_samples // self.client_average_factor()
        )
        duration = capture_samples / NOMINAL_ADC_SPS
        self.history = np.zeros(self.history_samples, dtype=np.uint16)
        self.x_data = np.linspace(-duration, 0.0, self.history_samples)
        self.history_sample_rate = (
            self.live_effective_sample_rate / self.client_average_factor()
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

    def apply_y_axis_range(self, volts_per_division=None, center_volts=None):
        if volts_per_division is None:
            volts_per_division = self.y_scale.currentData()
        if center_volts is None:
            center_volts = self.y_position.value()
        volts_per_division = max(0.01, volts_per_division)
        half_range_counts = (
            volts_per_division * Y_AXIS_DIVISIONS * 0.5
            * 4096 / VSCALE
        )
        center_counts = center_volts * 4096 / VSCALE
        self.plot_widget.setYRange(
            center_counts - half_range_counts,
            center_counts + half_range_counts,
            padding=0,
        )

    def on_manual_y_change(self, *_):
        if self.auto_y_button.isChecked():
            self.auto_y_button.setChecked(False)
        self.apply_y_axis_range()

    def zoom_y_axis(self, direction):
        next_index = min(
            len(self.y_scale_steps) - 1,
            max(0, self.y_scale.currentIndex() - direction),
        )
        if next_index != self.y_scale.currentIndex():
            self.y_scale.setCurrentIndex(next_index)

    def on_plot_range_changed(self, _view_box, ranges, *_):
        if self.auto_y_button.isChecked():
            return
        y_range = ranges[1]
        center_volts = (
            (y_range[0] + y_range[1]) * 0.5 * VSCALE / 4096
        )
        was_blocked = self.y_position.blockSignals(True)
        self.y_position.setValue(center_volts)
        self.y_position.blockSignals(was_blocked)
        volts_per_division = (
            (y_range[1] - y_range[0]) * VSCALE / 4096
            / Y_AXIS_DIVISIONS
        )
        nearest_index = min(
            range(len(self.y_scale_steps)),
            key=lambda index: abs(
                np.log(max(volts_per_division, 1e-9)
                       / self.y_scale_steps[index])
            ),
        )
        was_blocked = self.y_scale.blockSignals(True)
        self.y_scale.setCurrentIndex(nearest_index)
        self.y_scale.blockSignals(was_blocked)

    def on_autoscale_toggled(self, enabled):
        self.autoscale_envelope.clear()
        now = time.monotonic()
        self.autoscale_last_update = now
        self.autoscale_scale = self.y_scale.currentData()
        y_range = self.plot_widget.getViewBox().viewRange()[1]
        self.autoscale_position = (
            (y_range[0] + y_range[1]) * 0.5 * VSCALE / 4096
        )
        if enabled and len(self.history):
            self.record_autoscale_samples(self.history, now)
            self.update_autoscale(now)

    def record_autoscale_samples(self, samples, now=None):
        if not self.auto_y_button.isChecked() or len(samples) == 0:
            return
        if now is None:
            now = time.monotonic()
        self.autoscale_envelope.append(
            (now, int(np.min(samples)), int(np.max(samples)))
        )

    def update_autoscale(self, now=None):
        if not self.auto_y_button.isChecked() or not self.autoscale_envelope:
            return
        if now is None:
            now = time.monotonic()
        cutoff = now - AUTOSCALE_WINDOW_SECONDS
        while (
            self.autoscale_envelope
            and self.autoscale_envelope[0][0] < cutoff
        ):
            self.autoscale_envelope.popleft()
        if not self.autoscale_envelope:
            return

        minimum = min(entry[1] for entry in self.autoscale_envelope)
        maximum = max(entry[2] for entry in self.autoscale_envelope)
        span_counts = max(32.0, (maximum - minimum) * 1.2)
        raw_target_scale = (
            span_counts * VSCALE / 4096 / Y_AXIS_DIVISIONS
        )
        target_scale = min(
            self.y_scale_steps,
            key=lambda scale: abs(np.log(max(raw_target_scale, 1e-9) / scale)),
        )
        target_position = (minimum + maximum) * 0.5 * VSCALE / 4096

        previous_update = self.autoscale_last_update
        elapsed = (
            0.0 if previous_update is None
            else max(0.0, now - previous_update)
        )
        self.autoscale_last_update = now
        alpha = 1.0 - np.exp(-elapsed / AUTOSCALE_SMOOTHING_SECONDS)
        self.autoscale_scale += alpha * (
            target_scale - self.autoscale_scale
        )
        self.autoscale_position += alpha * (
            target_position - self.autoscale_position
        )

        index = min(
            range(len(self.y_scale_steps)),
            key=lambda item: abs(
                np.log(max(self.autoscale_scale, 1e-9)
                       / self.y_scale_steps[item])
            ),
        )
        was_blocked = self.y_scale.blockSignals(True)
        self.y_scale.setCurrentIndex(index)
        self.y_scale.blockSignals(was_blocked)
        was_blocked = self.y_position.blockSignals(True)
        self.y_position.setValue(self.autoscale_position)
        self.y_position.blockSignals(was_blocked)
        self.apply_y_axis_range(
            self.autoscale_scale, self.autoscale_position
        )

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
        peak_indices = np.flatnonzero(
            (spectrum[1:-1] >= spectrum[:-2])
            & (spectrum[1:-1] > spectrum[2:])
        ) + 1
        peak_indices = peak_indices[
            (peak_indices > 0) & (peak_indices < len(spectrum) - 1)
        ]
        ranked_indices = peak_indices[
            np.argsort(spectrum[peak_indices])[::-1]
        ]
        selected_indices = []
        for index in ranked_indices:
            if all(abs(index - selected) > 2 for selected in selected_indices):
                selected_indices.append(index)
                if len(selected_indices) == len(self.spectrum_peak_labels):
                    break

        peak_frequencies = frequencies[selected_indices]
        peak_amplitudes = spectrum[selected_indices]
        self.spectrum_peaks.setData(peak_frequencies, peak_amplitudes)
        for position, label in enumerate(self.spectrum_peak_labels):
            if position >= len(selected_indices):
                label.hide()
                continue
            frequency = peak_frequencies[position]
            frequency_label = (
                f"{frequency / 1000:.2f} kHz"
                if frequency >= 1000
                else f"{frequency:.0f} Hz"
            )
            label.setText(frequency_label)
            label.setPos(frequency, peak_amplitudes[position])
            label.show()

    def keep_spectrum_y_zero(self, view_box, ranges):
        y_minimum, y_maximum = ranges[1]
        if y_minimum < 0 or y_minimum > 0:
            view_box.setYRange(0, max(y_maximum, 1e-9), padding=0)

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

    def save_mode_settings(self, mode):
        self.mode_settings[mode] = {
            "sample_rate": self.sample_rate_value(),
            "capture_samples": self.capture_length_samples(),
        }

    def on_mode_changed(self, button, checked):
        if not checked:
            return
        new_mode = self.mode_group.id(button)
        if new_mode == self.active_settings_mode:
            return

        self.save_mode_settings(self.active_settings_mode)
        self.active_settings_mode = new_mode
        settings = self.mode_settings[new_mode]
        sample_rate = settings["sample_rate"]
        sample_rate_index = self.sample_rate.findData(sample_rate)
        if sample_rate_index < 0:
            raise ValueError(f"Unsupported saved sample rate: {sample_rate}")

        for widget in (self.sample_rate, self.capture_length_ms):
            widget.blockSignals(True)
        try:
            self.sample_rate.setCurrentIndex(sample_rate_index)
            self.selected_sample_rate = sample_rate
            self.capture_length_ms.setRange(
                SAMPLE_BUFFER_SIZE / sample_rate * 1000,
                MAX_CAPTURE_SAMPLES / sample_rate * 1000,
            )
            self.capture_length_ms.setSingleStep(
                SAMPLE_BUFFER_SIZE / sample_rate * 1000
            )
            self.snapped_capture_length_samples = settings["capture_samples"]
            self.capture_length_ms.setValue(
                settings["capture_samples"] / sample_rate * 1000
            )
        finally:
            self.capture_length_ms.blockSignals(False)
            self.sample_rate.blockSignals(False)

        self.live_effective_sample_rate = sample_rate
        self.refresh_filter_coefficients()
        self.refresh_acquisition_time_window()
        self.refresh_trigger_line()
        self.refresh_once_button()
        self.send_settings()

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
        self.live_effective_sample_rate = sample_rate
        self.refresh_filter_coefficients()
        self.snapped_capture_length_samples = None
        if self.last_raw_capture is None:
            self.history_sample_rate = (
                sample_rate / self.client_average_factor()
            )
        self.capture_length_ms.setRange(
            SAMPLE_BUFFER_SIZE / sample_rate * 1000,
            MAX_CAPTURE_SAMPLES / sample_rate * 1000,
        )
        self.capture_length_ms.setSingleStep(
            SAMPLE_BUFFER_SIZE / sample_rate * 1000
        )
        self.normalize_capture_length()
        self.refresh_acquisition_time_window()

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

    @staticmethod
    def parse_filter_settings(text):
        specs = []
        for line_number, raw_line in enumerate(text.splitlines(), start=1):
            line = raw_line.split("#", 1)[0].strip()
            if not line:
                continue
            fields = line.split()
            kind = fields[0].lower()
            if kind in ("highpass", "lowpass"):
                if len(fields) != 3:
                    raise ValueError(
                        f"Line {line_number}: use "
                        f"{kind} <cutoff Hz> <order 1-10>"
                    )
                try:
                    frequency = float(fields[1])
                    order = int(fields[2])
                except ValueError as error:
                    raise ValueError(
                        f"Line {line_number}: cutoff must be a number "
                        "and order must be an integer"
                    ) from error
                if not np.isfinite(frequency) or frequency <= 0:
                    raise ValueError(
                        f"Line {line_number}: cutoff must be positive"
                    )
                if not 1 <= order <= 10:
                    raise ValueError(
                        f"Line {line_number}: order must be from 1 to 10"
                    )
                specs.append((kind, frequency, order))
            elif kind == "notch":
                if len(fields) != 3:
                    raise ValueError(
                        f"Line {line_number}: use notch <center Hz> <Q>"
                    )
                try:
                    frequency, quality = map(float, fields[1:])
                except ValueError as error:
                    raise ValueError(
                        f"Line {line_number}: center frequency and Q "
                        "must be numbers"
                    ) from error
                if not np.isfinite(frequency) or frequency <= 0:
                    raise ValueError(
                        f"Line {line_number}: center frequency must be positive"
                    )
                if not np.isfinite(quality) or quality <= 0:
                    raise ValueError(
                        f"Line {line_number}: Q must be positive"
                    )
                specs.append((kind, frequency, quality))
            else:
                raise ValueError(
                    f"Line {line_number}: unsupported filter '{fields[0]}'"
                )
        return tuple(specs)

    @staticmethod
    def design_filter_chain(specs, sample_rate):
        sections = []
        nyquist = sample_rate / 2.0
        for kind, frequency, parameter in specs:
            if not 0 < frequency < nyquist:
                raise ValueError(
                    f"{kind} frequency {frequency:g} Hz must be below "
                    f"the Nyquist frequency ({nyquist:g} Hz)"
                )
            if kind == "notch":
                numerator, denominator = signal.iirnotch(
                    frequency, parameter, fs=sample_rate
                )
                sections.append(
                    signal.tf2sos(numerator, denominator)
                )
            else:
                sections.append(
                    signal.butter(
                        int(parameter), frequency, btype=kind,
                        fs=sample_rate, output="sos",
                    )
                )
        return np.vstack(sections) if sections else None

    def apply_sample_filters(self, samples, streaming=False):
        if (
            not self.filters_enabled_checkbox.isChecked()
            or self.filter_sos is None
            or len(samples) == 0
        ):
            return samples
        values = np.asarray(samples, dtype=np.float64)
        if streaming:
            if self.live_filter_state is None:
                self.live_filter_state = signal.sosfilt_zi(
                    self.filter_sos
                ) * values[0]
            filtered, self.live_filter_state = signal.sosfilt(
                self.filter_sos, values, zi=self.live_filter_state
            )
        else:
            initial_state = signal.sosfilt_zi(self.filter_sos) * values[0]
            filtered, _ = signal.sosfilt(
                self.filter_sos, values, zi=initial_state
            )
        return filtered

    def apply_capture_filters(self, samples, sample_rate):
        if (
            not self.filters_enabled_checkbox.isChecked()
            or not self.filter_specs
            or len(samples) == 0
        ):
            return samples
        try:
            sos = self.design_filter_chain(
                self.filter_specs,
                sample_rate / self.client_average_factor(),
            )
        except ValueError as error:
            self.filter_status_label.setText(str(error))
            self.filter_status_label.setStyleSheet("color: #ff7777;")
            return samples
        if sos is None:
            return samples
        values = np.asarray(samples, dtype=np.float64)
        initial_state = signal.sosfilt_zi(sos) * values[0]
        filtered, _ = signal.sosfilt(sos, values, zi=initial_state)
        return filtered

    def refresh_filter_coefficients(self):
        try:
            self.filter_sos = self.design_filter_chain(
                self.filter_specs,
                self.sample_rate_value() / self.client_average_factor(),
            )
        except ValueError as error:
            self.filter_sos = None
            self.filter_status_label.setText(str(error))
            self.filter_status_label.setStyleSheet("color: #ff7777;")
        else:
            self.filter_status_label.setStyleSheet("color: #aaaaaa;")
            self.update_filter_status()
        self.live_filter_state = None

    def apply_filter_settings(self, *_):
        try:
            specs = self.parse_filter_settings(
                self.filter_editor.toPlainText()
            )
            sample_rate = (
                self.sample_rate_value() / self.client_average_factor()
            )
            sos = self.design_filter_chain(specs, sample_rate)
        except ValueError as error:
            self.filter_status_label.setText(str(error))
            self.filter_status_label.setStyleSheet("color: #ff7777;")
            if self.filter_dialog_error is not None:
                self.filter_dialog_error.setText(str(error))
            return

        self.filter_text = self.filter_editor.toPlainText()
        self.filter_specs = specs
        self.filter_sos = sos
        self.live_filter_state = None
        self.filters_enabled_checkbox.blockSignals(True)
        self.filters_enabled_checkbox.setChecked(bool(specs))
        self.filters_enabled_checkbox.blockSignals(False)
        self.filter_status_label.setStyleSheet("color: #aaaaaa;")
        self.update_filter_status()
        if self.filter_dialog_error is not None:
            self.filter_dialog_error.clear()
        if self.last_raw_capture is not None:
            samples, buffer_capacity, overflows, trigger_index, sample_rate, \
                transfer_seconds, payload_length = self.last_raw_capture
            self.display_capture(
                samples, buffer_capacity, overflows, trigger_index, sample_rate,
                transfer_seconds, payload_length, new_capture=False,
            )
        else:
            self.history.fill(0)
            self.curve.setData(self.x_data, self.history)
            if self.plot_view.currentIndex() == 1:
                self.update_spectrum()
        self.filter_dialog.accept()

    def update_filter_status(self):
        if not self.filter_specs:
            message = "No client filters configured"
        elif not self.filters_enabled_checkbox.isChecked():
            message = f"{len(self.filter_specs)} filter(s) bypassed"
        else:
            message = f"{len(self.filter_specs)} client filter(s) active"
        self.filter_status_label.setText(message)

    def on_filters_enabled_changed(self, _enabled):
        self.live_filter_state = None
        self.update_filter_status()
        if self.last_raw_capture is not None:
            samples, buffer_capacity, overflows, trigger_index, sample_rate, \
                transfer_seconds, payload_length = self.last_raw_capture
            self.display_capture(
                samples, buffer_capacity, overflows, trigger_index, sample_rate,
                transfer_seconds, payload_length, new_capture=False,
            )
            return
        self.history.fill(0)
        self.curve.setData(self.x_data, self.history)
        if self.plot_view.currentIndex() == 1:
            self.update_spectrum()

    def open_filter_dialog(self, *_):
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle("Client-side filters")
        dialog.setMinimumWidth(440)
        layout = QtWidgets.QVBoxLayout(dialog)
        instructions = QtWidgets.QLabel(
            "Enter one filter per line. Filters cascade from top to bottom.\n"
            "highpass <cutoff Hz> <order 1-10>\n"
            "lowpass <cutoff Hz> <order 1-10>\n"
            "notch <center Hz> <Q>\n"
            "Use # for comments. Frequencies must be below the Nyquist "
            "frequency. These filters affect only the client display."
        )
        instructions.setWordWrap(True)
        layout.addWidget(instructions)
        editor = QtWidgets.QPlainTextEdit()
        editor.setPlaceholderText(
            "highpass 50 2\nlowpass 10000 4\nnotch 60 30"
        )
        editor.setPlainText(self.filter_text)
        layout.addWidget(editor)
        self.filter_editor = editor
        self.filter_dialog = dialog
        self.filter_dialog_error = QtWidgets.QLabel()
        self.filter_dialog_error.setStyleSheet("color: #ff7777;")
        self.filter_dialog_error.setWordWrap(True)
        layout.addWidget(self.filter_dialog_error)
        button_box = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.StandardButton.Apply
            | QtWidgets.QDialogButtonBox.StandardButton.Cancel
        )
        button_box.button(
            QtWidgets.QDialogButtonBox.StandardButton.Apply
        ).clicked.connect(self.apply_filter_settings)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)
        dialog.finished.connect(self.close_filter_dialog)
        dialog.exec()

    def close_filter_dialog(self, result):
        if result != QtWidgets.QDialog.DialogCode.Accepted:
            self.refresh_filter_coefficients()
        self.filter_editor = None
        self.filter_dialog = None
        self.filter_dialog_error = None

    def refresh_client_average(self, *_):
        factor = self.client_average_factor()
        self.refresh_filter_coefficients()
        if self.last_raw_capture is not None:
            samples, buffer_capacity, overflows, trigger_index, sample_rate, \
                transfer_seconds, payload_length = self.last_raw_capture
            self.display_capture(
                samples, buffer_capacity, overflows, trigger_index, sample_rate,
                transfer_seconds, payload_length, new_capture=False,
            )
            return
        self.history_samples = max(
            1, self.capture_length_samples() // factor
        )
        self.history = np.zeros(self.history_samples, dtype=np.uint16)
        duration = self.capture_length_samples() / self.sample_rate_value()
        self.x_data = np.linspace(-duration, 0.0, self.history_samples)
        self.history_sample_rate = (
            self.live_effective_sample_rate / factor
        )
        self.refresh_acquisition_time_window()
        self.curve.setData(self.x_data, self.history)
        if self.plot_view.currentIndex() == 1:
            self.update_spectrum()

    def capture_length_samples(self):
        if self.snapped_capture_length_samples is not None:
            return self.snapped_capture_length_samples
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
        samples = self.capture_length_samples()
        self.snapped_capture_length_samples = samples
        snapped_duration_ms = samples / self.sample_rate_value() * 1000
        if abs(self.capture_length_ms.value() - snapped_duration_ms) >= 0.05:
            self.normalizing_capture_length = True
            try:
                self.capture_length_ms.setValue(snapped_duration_ms)
            finally:
                self.normalizing_capture_length = False
        self.save_mode_settings(self.active_settings_mode)

    def on_capture_length_changed(self):
        if self.normalizing_capture_length:
            self.refresh_acquisition_time_window()
            return
        self.snapped_capture_length_samples = None
        self.refresh_acquisition_time_window()
        self.save_mode_settings(self.active_settings_mode)
        self.send_settings()

    def refresh_acquisition_time_window(self, *_):
        sample_rate = self.sample_rate_value()
        capture_samples = self.capture_length_samples()
        factor = self.client_average_factor()
        self.history_samples = max(1, capture_samples // factor)
        duration = capture_samples / sample_rate
        if self.last_raw_capture is None:
            if len(self.history) != self.history_samples:
                retained = self.history[-self.history_samples:]
                self.history = np.zeros(self.history_samples, dtype=np.uint16)
                if len(retained):
                    self.history[-len(retained):] = retained
            self.x_data = np.linspace(
                -duration, 0.0, self.history_samples
            )
            self.history_sample_rate = self.live_effective_sample_rate / factor
            self.curve.setData(self.x_data, self.history)
            if self.plot_view.currentIndex() == 1:
                self.update_spectrum()
        else:
            self.capture_view_initialized = False

        if self.mode_index() == 1:
            pretrigger = self.trigger_offset.value() / 100.0
            self.plot_widget.setXRange(
                -pretrigger * duration,
                (1.0 - pretrigger) * duration,
                padding=0,
            )
        else:
            self.plot_widget.setXRange(-duration, 0.0, padding=0)

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
        self.save_mode_settings(self.active_settings_mode)
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
        effective_sample_rate = sample_rate
        if overflows:
            effective_sample_rate *= len(samples) / (
                len(samples) + overflows * SAMPLE_BUFFER_SIZE
            )
        factor = self.client_average_factor()
        averaged = self.average_for_display(samples)
        averaged = self.apply_capture_filters(
            averaged, effective_sample_rate
        )
        self.last_capture_length = len(samples)
        self.last_capture_overflows = overflows
        self.last_capture_seconds = transfer_seconds
        self.history = averaged
        self.history_sample_rate = effective_sample_rate / factor
        displayed_trigger_index = (
            (trigger_index - (factor - 1) / 2) / factor
            if trigger_index != 0xFFFFFFFF else None
        )
        if displayed_trigger_index is None:
            self.x_data = (
                np.arange(len(averaged)) * factor + (factor - 1) / 2
            ) / effective_sample_rate
            self.plot_widget.setLabel("bottom", "Sample time", units="s")
        else:
            self.x_data = (
                np.arange(len(averaged)) * factor + (factor - 1) / 2 -
                trigger_index
            ) / effective_sample_rate
            self.plot_widget.setLabel("bottom", "Time from trigger", units="s")
        if not self.capture_view_initialized and len(self.x_data):
            self.plot_widget.setXRange(self.x_data[0], self.x_data[-1])
            self.capture_view_initialized = True
        self.curve.setData(self.x_data, self.history)
        if new_capture:
            self.record_autoscale_samples(averaged)
        self.update_autoscale()
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
            self.update_autoscale()
            return

        frames = []
        while not frame_queue.empty():
            frames.append(frame_queue.get_nowait())

        if not frames:
            self.update_autoscale()
            return

        new_data = np.concatenate(frames)
        new_data = self.average_for_display(new_data)
        new_data = self.apply_sample_filters(new_data, streaming=True)
        self.record_autoscale_samples(new_data)
        n = len(new_data)
        self.last_raw_capture = None
        self.history_sample_rate = (
            self.live_effective_sample_rate / self.client_average_factor()
        )

        if len(self.history) != self.history_samples:
            self.history = np.zeros(self.history_samples, dtype=np.uint16)
            duration = self.capture_length_samples() / self.sample_rate_value()
            self.x_data = np.linspace(-duration, 0.0, self.history_samples)
            self.plot_widget.setXRange(-duration, 0.0)
        if n >= self.history_samples:
            self.history[:] = new_data[-self.history_samples:]
        else:
            self.history = np.roll(self.history, -n)
            self.history[-n:] = new_data

        self.curve.setData(self.x_data, self.history)
        self.update_autoscale()
        if self.plot_view.currentIndex() == 1:
            self.update_spectrum()

    def update_benchmark(self):
        now = time.perf_counter()
        dt = now - self.last_time

        with stats_lock:
            current_frames = rx_frame_count
            current_bytes = rx_byte_count
            current_rx_packets = rx_packet_count
            current_rx_incomplete = rx_incomplete_count
            current_tx_packets = device_tx_packet_count
            current_tx_errors = device_tx_error_count
            current_stream_blocks = device_stream_block_count
            current_dma_drops = device_dma_drop_count

        d_frames = current_frames - self.last_frames
        d_bytes = current_bytes - self.last_bytes
        d_rx_packets = current_rx_packets - self.last_rx_packets
        if d_rx_packets < 0:
            d_rx_packets = current_rx_packets

        self.last_time = now
        self.last_frames = current_frames
        self.last_bytes = current_bytes
        self.last_rx_packets = current_rx_packets

        if dt > 0:
            fps = d_frames / dt
            effective_sps = fps * TOTAL_SAMPLES
            effective_ksps = effective_sps / 1000.0
            nominal_ksps = self.sample_rate_value() / 1000.0

            duty_cycle = (effective_sps / self.sample_rate_value()) * 100.0
            mbps = (d_bytes * 8) / (dt * 1e6)
            rx_packets_per_sec = d_rx_packets / dt
            if self.mode_index() == 0 and d_frames > 0:
                self.live_effective_sample_rate = min(
                    self.sample_rate_value(), effective_sps
                )

            msg = (
                f"{duty_cycle:.1f}% of {nominal_ksps:.0f} kSps  |  "
                f"{effective_ksps:.1f} kS/s ({fps:.0f} FPS)  |  "
                f"{mbps:.2f} Mbps  |  "
                f"TX ~1s {current_stream_blocks}blk "
                f"{current_tx_packets}pkt {current_tx_errors}err  |  "
                f"RX {rx_packets_per_sec:.0f} pkt/s, "
                f"DMA drop {current_dma_drops}, "
                f"{current_rx_incomplete} partial total"
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
