import socket
import struct
import threading
import queue
import time
import numpy as np
import pyqtgraph as pg
from PyQt6 import QtCore, QtWidgets

PORT = 4444
CONTROL_PORT = 4445
CAPTURE_PORT = 4446
STATUS_PORT = 4447
STATUS_FLASH_MIN_INTERVAL = 1 / 60
STATUS_FLASH_DURATION_MS = 100
TOTAL_SAMPLES = 1024
SAMPLES_PER_PACKET = 512
NUM_PARTS = TOTAL_SAMPLES // SAMPLES_PER_PACKET
NUM_RING_BUFFERS = 84
SAMPLE_BUFFER_SIZE = 1024
MAX_CAPTURE_SAMPLES = NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE

NOMINAL_ADC_SPS = 500_000
# HISTORY_SEC = 0.1
HISTORY_SEC = (TOTAL_SAMPLES*2) / NOMINAL_ADC_SPS
HISTORY_SAMPLES = int(HISTORY_SEC * NOMINAL_ADC_SPS)

# 3.3V ADC Max + 1/2 Voltage divider -> VSCALE = Voltage at Max ADC
VSCALE = 3.3 / (1/2)

frame_queue = queue.Queue(maxsize=100)
capture_queue = queue.Queue()

stats_lock = threading.Lock()
rx_frame_count = 0
rx_byte_count = 0
viewer_address = None

class StatusEvents(QtCore.QObject):
    trigger_hit = QtCore.pyqtSignal(int)
    overflow = QtCore.pyqtSignal(int)

status_events = StatusEvents()

def udp_worker():
    global rx_frame_count, rx_byte_count, viewer_address
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", PORT))
    frames = {}

    while True:
        packet, sender = sock.recvfrom(2048)
        viewer_address = sender[0]
        sample_count, sample_part = struct.unpack_from("<IB", packet, 0)
        data_offset = len(packet) - (SAMPLES_PER_PACKET * 2)
        samples = np.frombuffer(packet, dtype=np.uint16, offset=data_offset, count=SAMPLES_PER_PACKET)

        if sample_count not in frames:
            frames[sample_count] = {}

        frames[sample_count][sample_part] = samples

        if len(frames[sample_count]) == NUM_PARTS:
            full_frame = np.concatenate([frames[sample_count][p] for p in range(NUM_PARTS)])
            del frames[sample_count]

            with stats_lock:
                rx_frame_count += 1
                rx_byte_count += len(packet) * NUM_PARTS

            if frame_queue.full():
                try:
                    frame_queue.get_nowait()
                except queue.Empty:
                    pass

            frame_queue.put(full_frame)

        if len(frames) > 20:
            del frames[min(frames.keys())]

def recv_exact(connection, length):
    data = bytearray(length)
    view = memoryview(data)
    offset = 0
    while offset < length:
        received = connection.recv_into(view[offset:], length - offset)
        if not received:
            return None
        offset += received
    return data

def recv_samples(connection, sample_count):
    samples = np.empty(sample_count, dtype="<u2")
    payload = memoryview(samples).cast("B")
    offset = 0
    while offset < len(payload):
        received = connection.recv_into(payload[offset:], len(payload) - offset)
        if not received:
            return None
        offset += received
    return samples

def tcp_capture_worker():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", CAPTURE_PORT))
    server.listen()

    while True:
        connection, _ = server.accept()
        try:
            with connection:
                while True:
                    transfer_started = time.perf_counter()
                    header = recv_exact(connection, 20)
                    if header is None:
                        break
                    magic, sample_count, buffer_capacity, overflow_count, trigger_index = struct.unpack(
                        "<4sIIII", header
                    )
                    if (
                        magic != b"OSCP"
                        or not SAMPLE_BUFFER_SIZE <= buffer_capacity <= MAX_CAPTURE_SAMPLES
                        or sample_count > buffer_capacity
                        or (sample_count and trigger_index != 0xFFFFFFFF
                            and trigger_index >= sample_count)
                    ):
                        capture_queue.put(
                            (None, 0, 0, None, 0.0, "Invalid capture header from oscilloscope")
                        )
                        break

                    if sample_count == 0:
                        capture_queue.put(
                            (None, buffer_capacity, overflow_count, trigger_index, 0.0, None)
                        )
                        continue

                    samples = recv_samples(connection, sample_count)
                    if samples is None:
                        capture_queue.put(
                            (None, 0, 0, None, 0.0, "Incomplete TCP capture received")
                        )
                        break
                    transfer_seconds = time.perf_counter() - transfer_started
                    capture_queue.put(
                        (samples, buffer_capacity, overflow_count, trigger_index,
                         transfer_seconds, None)
                    )
        except OSError as error:
            capture_queue.put(
                (None, 0, 0, None, 0.0, f"Capture TCP connection reset: {error}")
            )

def status_worker():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", STATUS_PORT))
    while True:
        packet, _ = sock.recvfrom(64)
        if len(packet) != 9 or packet[:4] != b"OSCE":
            continue
        event, value = struct.unpack_from("<BI", packet, 4)
        if event == 1:
            status_events.trigger_hit.emit(value)
        elif event == 2:
            status_events.overflow.emit(value)

class LivePlot(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Oscilloscope")
        self.resize(1180, 680)
        self.setMinimumSize(900, 540)

        self.history_samples = HISTORY_SAMPLES
        self.history = np.zeros(self.history_samples, dtype=np.uint16)
        self.x_data = np.linspace(-HISTORY_SEC, 0.0, self.history_samples)

        self.plot_widget = pg.PlotWidget()
        self.plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self.plot_widget.setYRange(0, 4096)
        self.plot_widget.setXRange(-HISTORY_SEC, 0.0)
        self.plot_widget.setLabel("bottom", "Time", units="s")
        self.plot_widget.setLabel("left", "Voltage", units="V")
        self.plot_widget.getAxis("left").setScale(VSCALE / 4096.0)
        self.curve = self.plot_widget.plot(pen=pg.mkPen(color="#00ffff", width=1.5))
        self.trigger_line = pg.InfiniteLine(
            angle=0, movable=False,
            pen=pg.mkPen(color="#ff8800", width=1, style=QtCore.Qt.PenStyle.DashLine)
        )
        self.trigger_line.setZValue(10)
        self.plot_widget.addItem(self.trigger_line)

        self.mode = QtWidgets.QComboBox()
        self.mode.addItems(["Stream samples", "Capture on trigger"])
        self.sample_rate = QtWidgets.QSpinBox()
        self.sample_rate.setRange(8, NOMINAL_ADC_SPS)
        self.sample_rate.setValue(NOMINAL_ADC_SPS)
        self.sample_rate.setSuffix(" S/s")
        self.trigger_voltage = QtWidgets.QDoubleSpinBox()
        self.trigger_voltage.setRange(0.0, VSCALE)
        self.trigger_voltage.setDecimals(2)
        self.trigger_voltage.setValue(VSCALE / 2)
        self.trigger_voltage.setSuffix(" V")
        self.edge = QtWidgets.QComboBox()
        self.edge.addItems(["Rising edge", "Falling edge"])
        self.trigger_repeat = QtWidgets.QComboBox()
        self.trigger_repeat.addItems(["Trigger once", "Trigger continuously"])
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
        self.capture_details = QtWidgets.QLabel("No capture received")
        self.capture_details.setWordWrap(True)
        self.last_capture_length = 0
        self.last_capture_overflows = 0
        self.last_capture_seconds = 0.0
        self.last_trigger_flash_time = 0.0
        self.last_overflow_flash_time = 0.0
        self.capture_count = 0
        self.capture_rate = 0.0

        acquisition_group = QtWidgets.QGroupBox("Acquisition")
        acquisition_form = QtWidgets.QFormLayout(acquisition_group)
        acquisition_form.setLabelAlignment(QtCore.Qt.AlignmentFlag.AlignLeft)
        acquisition_form.addRow("Run mode", self.mode)
        acquisition_form.addRow("Sample rate", self.sample_rate)
        acquisition_form.addRow("Capture length", self.capture_length_ms)

        trigger_group = QtWidgets.QGroupBox("Trigger")
        trigger_form = QtWidgets.QFormLayout(trigger_group)
        trigger_form.addRow("Level", self.trigger_voltage)
        trigger_form.addRow("Edge", self.edge)
        trigger_form.addRow("Repeat", self.trigger_repeat)
        trigger_form.addRow("Pre-trigger", self.trigger_offset)

        self.capture_once_button = QtWidgets.QPushButton("Capture once")
        self.capture_once_button.setMinimumHeight(34)
        self.apply_button = QtWidgets.QPushButton("Apply settings")
        self.trigger_indicator = QtWidgets.QLabel("Trigger: idle")
        self.overflow_indicator = QtWidgets.QLabel("Overflow: none")
        self.trigger_indicator.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.overflow_indicator.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        for indicator in (self.trigger_indicator, self.overflow_indicator):
            indicator.setMinimumHeight(32)
            indicator.setFrameStyle(
                QtWidgets.QFrame.Shape.Box | QtWidgets.QFrame.Shadow.Plain
            )
            indicator.setLineWidth(1)

        indicator_row = QtWidgets.QHBoxLayout()
        indicator_row.addWidget(self.trigger_indicator)
        indicator_row.addWidget(self.overflow_indicator)

        self.capture_rate_label = QtWidgets.QLabel("Captures/s: 0.00")
        self.last_capture_label = QtWidgets.QLabel("Last capture: --")
        self.capture_count_label = QtWidgets.QLabel("Total captures: 0")
        self.capture_rate_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignLeft)
        self.last_capture_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignLeft)
        self.capture_count_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignLeft)
        capture_stats = QtWidgets.QFormLayout()
        capture_stats.addRow("Capture rate", self.capture_rate_label)
        capture_stats.addRow("Last capture", self.last_capture_label)
        capture_stats.addRow("Total", self.capture_count_label)

        controls = QtWidgets.QWidget()
        controls.setMinimumWidth(280)
        controls.setMaximumWidth(370)
        controls_layout = QtWidgets.QVBoxLayout(controls)
        controls_layout.setContentsMargins(8, 8, 8, 8)
        controls_layout.addWidget(acquisition_group)
        controls_layout.addWidget(self.capture_once_button)
        controls_layout.addWidget(trigger_group)
        controls_layout.addWidget(self.apply_button)
        controls_layout.addLayout(indicator_row)
        controls_layout.addLayout(capture_stats)
        controls_layout.addWidget(self.capture_details)
        controls_layout.addStretch(1)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        splitter.addWidget(self.plot_widget)
        splitter.addWidget(controls)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 0)
        splitter.setSizes([850, 330])
        self.setCentralWidget(splitter)

        self.refresh_trigger_line()
        status_events.trigger_hit.connect(
            self.on_trigger_hit, QtCore.Qt.ConnectionType.QueuedConnection
        )
        status_events.overflow.connect(
            self.on_overflow, QtCore.Qt.ConnectionType.QueuedConnection
        )
        self.apply_button.clicked.connect(self.send_settings)
        self.capture_once_button.clicked.connect(
            lambda: self.send_settings(mode_override=1)
        )
        for widget in (
            self.mode, self.sample_rate, self.trigger_voltage, self.edge,
            self.trigger_repeat, self.capture_length_ms, self.trigger_offset,
        ):
            if isinstance(widget, QtWidgets.QComboBox):
                widget.currentIndexChanged.connect(self.send_settings)
            else:
                widget.valueChanged.connect(self.send_settings)
        self.sample_rate.valueChanged.connect(self.refresh_sample_rate)
        self.trigger_voltage.valueChanged.connect(self.refresh_trigger_line)
        self.mode.currentIndexChanged.connect(self.refresh_trigger_line)

        self.status = self.statusBar()
        self.status.setStyleSheet("font-size: 13px; font-weight: bold")
        self.status.showMessage("Benchmarking...")

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
        self.device_timer = QtCore.QTimer()
        self.device_timer.timeout.connect(self.check_device)
        self.device_timer.start(250)
        self.send_settings()

    def reset_trigger_indicator(self):
        self.trigger_indicator.setText("Trigger: idle")
        self.trigger_indicator.setStyleSheet("")

    def reset_overflow_indicator(self):
        if self.last_capture_overflows:
            self.overflow_indicator.setText(
                f"Overflow: {self.last_capture_overflows} buffer(s)"
            )
        else:
            self.overflow_indicator.setText("Overflow: none")
        self.overflow_indicator.setStyleSheet("")

    def on_trigger_hit(self, _value):
        now = time.monotonic()
        if now - self.last_trigger_flash_time < STATUS_FLASH_MIN_INTERVAL:
            return
        self.last_trigger_flash_time = now
        self.trigger_indicator.setText("Trigger: hit")
        self.trigger_indicator.setStyleSheet(
            "background-color: #fff2b3; color: #202020;"
        )
        self.trigger_flash_timer.start(STATUS_FLASH_DURATION_MS)
        self.capture_details.setText("Trigger edge detected.")

    def on_overflow(self, value):
        self.last_capture_overflows = max(self.last_capture_overflows, value)
        self.overflow_indicator.setText(f"Overflow: {value} buffer(s)")
        now = time.monotonic()
        if now - self.last_overflow_flash_time >= STATUS_FLASH_MIN_INTERVAL:
            self.last_overflow_flash_time = now
            self.overflow_indicator.setStyleSheet(
                "background-color: #ffb0b0; color: #202020;"
            )
            self.overflow_flash_timer.start(STATUS_FLASH_DURATION_MS)
        self.capture_details.setText(f"DMA overflow reported: {value} buffer(s).")

    def refresh_trigger_line(self, *_):
        adc_level = self.trigger_voltage.value() / VSCALE * 4096
        self.trigger_line.setValue(adc_level)
        self.trigger_line.setVisible(self.mode.currentIndex() == 1)

    def check_device(self):
        address = viewer_address
        if address is not None and address != self.last_control_address:
            self.last_control_address = address
            self.send_settings()

    def refresh_sample_rate(self, *_):
        duration = self.history_samples / self.sample_rate.value()
        self.x_data = np.linspace(-duration, 0.0, self.history_samples)
        self.plot_widget.setXRange(-duration, 0.0)
        self.capture_length_ms.setRange(
            SAMPLE_BUFFER_SIZE / self.sample_rate.value() * 1000,
            MAX_CAPTURE_SAMPLES / self.sample_rate.value() * 1000,
        )
        self.capture_length_ms.setSingleStep(
            SAMPLE_BUFFER_SIZE / self.sample_rate.value() * 1000
        )

    def send_settings(self, *_, mode_override=None):
        if viewer_address is None:
            self.status.showMessage("Waiting for oscilloscope UDP samples...")
            return

        mode = mode_override
        if mode is None:
            mode = 2 if self.mode.currentIndex() == 1 else 0
        requested_samples = round(
            self.capture_length_ms.value() * self.sample_rate.value() / 1000
        )
        capture_length = min(
            MAX_CAPTURE_SAMPLES,
            max(SAMPLE_BUFFER_SIZE, requested_samples),
        )
        capture_length = (
            (capture_length + SAMPLE_BUFFER_SIZE - 1) // SAMPLE_BUFFER_SIZE
        ) * SAMPLE_BUFFER_SIZE
        message = (
            f"{mode} {self.sample_rate.value()} "
            f"{self.trigger_voltage.value():.4f} {self.edge.currentIndex()} "
            f"{self.trigger_repeat.currentIndex()} {capture_length} "
            f"{self.trigger_offset.value()}"
        )
        try:
            self.control_socket.sendto(
                message.encode("ascii"), (viewer_address, CONTROL_PORT)
            )
            self.last_control_address = viewer_address
            if mode_override == 1:
                self.capture_details.setText(
                    f"One-shot requested: {capture_length:,} samples "
                    f"({capture_length / self.sample_rate.value() * 1000:.3f} ms)."
                )
        except OSError as error:
            self.status.showMessage(f"Could not send settings: {error}")

    def update_plot(self):
        capture_updated = False
        while not capture_queue.empty():
            (samples, buffer_capacity, overflows, trigger_index,
             transfer_seconds, error) = capture_queue.get_nowait()
            if error is not None:
                self.capture_details.setText(f"Capture error: {error}")
            elif samples is None:
                if self.last_capture_length:
                    self.last_capture_overflows = max(
                        self.last_capture_overflows, overflows
                    )
                    self.capture_details.setText(
                        f"Capture summary: {self.last_capture_length:,} samples, "
                        f"{self.last_capture_overflows} DMA overflow(s), "
                        f"TCP receive {self.last_capture_seconds:.3f}s."
                    )
            else:
                max_capture_ms = (
                    buffer_capacity / self.sample_rate.value() * 1000
                )
                self.capture_length_ms.setMaximum(max_capture_ms)
                if self.capture_length_ms.value() > max_capture_ms:
                    self.capture_length_ms.setValue(max_capture_ms)
                self.last_capture_length = len(samples)
                self.last_capture_overflows = overflows
                self.last_capture_seconds = transfer_seconds
                self.history = samples
                if trigger_index == 0xFFFFFFFF:
                    self.x_data = np.arange(len(samples)) / self.sample_rate.value()
                    self.plot_widget.setLabel("bottom", "Sample time", units="s")
                else:
                    self.x_data = (
                        np.arange(len(samples)) - trigger_index
                    ) / self.sample_rate.value()
                    self.plot_widget.setLabel("bottom", "Time from trigger", units="s")
                self.plot_widget.setXRange(self.x_data[0], self.x_data[-1])
                self.curve.setData(self.x_data, self.history)
                sample_duration = len(samples) / self.sample_rate.value()
                transfer_rate = (
                    len(samples) * 2 / transfer_seconds if transfer_seconds else 0
                )
                trigger_description = (
                    "not edge-triggered"
                    if trigger_index == 0xFFFFFFFF
                    else f"trigger at sample {trigger_index:,}"
                )
                self.capture_details.setText(
                    f"Capture received: {len(samples):,} samples "
                    f"({sample_duration * 1000:.3f} ms at {self.sample_rate.value():,} S/s)\n"
                    f"TCP: {transfer_seconds:.3f} s, {transfer_rate / 1e6:.2f} MB/s\n"
                    f"Trigger: {trigger_description}; DMA overflows: {overflows}; "
                    f"device capacity: {buffer_capacity:,} samples."
                )
                self.last_capture_overflows = overflows
                self.reset_overflow_indicator()
                self.capture_count += 1
                self.last_capture_label.setText(
                    f"Last capture: {transfer_seconds:.3f} s, "
                    f"{len(samples):,} samples"
                )
                self.capture_count_label.setText(
                    f"Total captures: {self.capture_count}"
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
        n = len(new_data)

        if len(self.history) != self.history_samples:
            self.history = np.zeros(self.history_samples, dtype=np.uint16)
            duration = self.history_samples / self.sample_rate.value()
            self.x_data = np.linspace(-duration, 0.0, self.history_samples)
            self.plot_widget.setXRange(-duration, 0.0)
        if n >= self.history_samples:
            self.history[:] = new_data[-self.history_samples:]
        else:
            self.history = np.roll(self.history, -n)
            self.history[-n:] = new_data

        self.curve.setData(self.x_data, self.history)

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
            nominal_ksps = self.sample_rate.value() / 1000.0

            duty_cycle = (effective_sps / self.sample_rate.value()) * 100.0
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

if __name__ == "__main__":
    t = threading.Thread(target=udp_worker, daemon=True)
    t.start()
    capture_thread = threading.Thread(target=tcp_capture_worker, daemon=True)
    capture_thread.start()
    status_thread = threading.Thread(target=status_worker, daemon=True)
    status_thread.start()

    app = QtWidgets.QApplication([])
    window = LivePlot()
    window.show()
    app.exec()
