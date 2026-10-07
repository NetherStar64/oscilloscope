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
TOTAL_SAMPLES = 1024
SAMPLES_PER_PACKET = 512
NUM_PARTS = TOTAL_SAMPLES // SAMPLES_PER_PACKET
NUM_RING_BUFFERS = 64
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
    data = bytearray()
    while len(data) < length:
        chunk = connection.recv(length - len(data))
        if not chunk:
            return None
        data.extend(chunk)
    return bytes(data)

def tcp_capture_worker():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", CAPTURE_PORT))
    server.listen()

    while True:
        connection, _ = server.accept()
        with connection:
            while True:
                header = recv_exact(connection, 16)
                if header is None:
                    break
                magic, sample_count, buffer_capacity, overflow_count = struct.unpack(
                    "<4sIII", header
                )
                if (
                    magic != b"OSCP"
                    or not SAMPLE_BUFFER_SIZE <= buffer_capacity <= MAX_CAPTURE_SAMPLES
                    or sample_count > buffer_capacity
                ):
                    capture_queue.put((None, 0, 0, "Invalid capture header from oscilloscope"))
                    break

                if sample_count == 0:
                    capture_queue.put((None, buffer_capacity, overflow_count, None))
                    continue

                payload = recv_exact(connection, sample_count * 2)
                if payload is None:
                    capture_queue.put((None, 0, 0, "Incomplete TCP capture received"))
                    break
                samples = np.frombuffer(payload, dtype="<u2").copy()
                capture_queue.put((samples, buffer_capacity, overflow_count, None))

class LivePlot(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Oscilloscope")
        self.resize(950, 550)

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

        controls = QtWidgets.QWidget()
        controls_layout = QtWidgets.QGridLayout(controls)
        controls_layout.setContentsMargins(6, 4, 6, 4)
        controls_layout.setHorizontalSpacing(8)
        controls_layout.setVerticalSpacing(6)
        self.mode = QtWidgets.QComboBox()
        self.mode.addItems(["Stream samples", "Capture once", "Capture on trigger"])
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
        self.max_capture_length = QtWidgets.QSpinBox()
        self.max_capture_length.setRange(SAMPLE_BUFFER_SIZE, MAX_CAPTURE_SAMPLES)
        self.max_capture_length.setSingleStep(SAMPLE_BUFFER_SIZE)
        self.max_capture_length.setValue(MAX_CAPTURE_SAMPLES)
        self.max_capture_length.setSuffix(" samples")
        self.capture_status = QtWidgets.QLabel("No capture received")
        self.last_capture_length = 0
        self.last_capture_overflows = 0
        controls_layout.addWidget(QtWidgets.QLabel("Mode"), 0, 0)
        controls_layout.addWidget(self.mode, 0, 1)
        controls_layout.addWidget(QtWidgets.QLabel("Sample rate"), 0, 2)
        controls_layout.addWidget(self.sample_rate, 0, 3)
        controls_layout.addWidget(QtWidgets.QLabel("Trigger level"), 0, 4)
        controls_layout.addWidget(self.trigger_voltage, 0, 5)
        controls_layout.addWidget(QtWidgets.QLabel("Trigger edge"), 1, 0)
        controls_layout.addWidget(self.edge, 1, 1)
        controls_layout.addWidget(QtWidgets.QLabel("Trigger mode"), 1, 2)
        controls_layout.addWidget(self.trigger_repeat, 1, 3)
        controls_layout.addWidget(QtWidgets.QLabel("Max length"), 1, 4)
        controls_layout.addWidget(self.max_capture_length, 1, 5)
        controls_layout.addWidget(self.capture_status, 2, 0, 1, 5)
        self.apply_button = QtWidgets.QPushButton("Apply")
        controls_layout.addWidget(self.apply_button, 2, 5)
        layout = QtWidgets.QVBoxLayout()
        layout.addWidget(self.plot_widget)
        layout.addWidget(controls)
        container = QtWidgets.QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)
        self.apply_button.clicked.connect(self.send_settings)
        for widget in (
            self.mode, self.sample_rate, self.trigger_voltage, self.edge,
            self.trigger_repeat, self.max_capture_length,
        ):
            if isinstance(widget, QtWidgets.QComboBox):
                widget.currentIndexChanged.connect(self.send_settings)
            else:
                widget.valueChanged.connect(self.send_settings)
        self.sample_rate.valueChanged.connect(self.refresh_sample_rate)

        self.status = self.statusBar()
        self.status.setStyleSheet("font-size: 13px; font-weight: bold")
        self.status.showMessage("Benchmarking...")

        self.last_time = time.perf_counter()
        self.last_frames = 0
        self.last_bytes = 0

        self.plot_timer = QtCore.QTimer()
        self.plot_timer.timeout.connect(self.update_plot)
        self.plot_timer.start(1000 // 60)

        self.bench_timer = QtCore.QTimer()
        self.bench_timer.timeout.connect(self.update_benchmark)
        self.bench_timer.start(1000)
        self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.last_control_address = None
        self.device_timer = QtCore.QTimer()
        self.device_timer.timeout.connect(self.check_device)
        self.device_timer.start(250)
        self.send_settings()

    def check_device(self):
        address = viewer_address
        if address is not None and address != self.last_control_address:
            self.last_control_address = address
            self.send_settings()

    def refresh_sample_rate(self, *_):
        duration = self.history_samples / self.sample_rate.value()
        self.x_data = np.linspace(-duration, 0.0, self.history_samples)
        self.plot_widget.setXRange(-duration, 0.0)

    def send_settings(self, *_):
        if viewer_address is None:
            self.status.showMessage("Waiting for oscilloscope UDP samples...")
            return

        message = (
            f"{self.mode.currentIndex()} {self.sample_rate.value()} "
            f"{self.trigger_voltage.value():.4f} {self.edge.currentIndex()} "
            f"{self.trigger_repeat.currentIndex()} {self.max_capture_length.value()}"
        )
        try:
            self.control_socket.sendto(
                message.encode("ascii"), (viewer_address, CONTROL_PORT)
            )
            self.last_control_address = viewer_address
        except OSError as error:
            self.status.showMessage(f"Could not send settings: {error}")

    def update_plot(self):
        capture_updated = False
        while not capture_queue.empty():
            samples, buffer_capacity, overflows, error = capture_queue.get_nowait()
            if error is not None:
                self.capture_status.setText(error)
            elif samples is None:
                if self.last_capture_length:
                    self.last_capture_overflows = max(
                        self.last_capture_overflows, overflows
                    )
                    self.capture_status.setText(
                        f"Captured {self.last_capture_length:,} samples; "
                        f"{self.last_capture_overflows} DMA buffer overflow(s)"
                    )
            else:
                self.max_capture_length.setMaximum(buffer_capacity)
                if self.max_capture_length.value() > buffer_capacity:
                    self.max_capture_length.setValue(buffer_capacity)
                self.last_capture_length = len(samples)
                self.last_capture_overflows = overflows
                self.history = samples
                self.x_data = np.linspace(
                    -len(samples) / self.sample_rate.value(), 0.0, len(samples)
                )
                self.plot_widget.setXRange(self.x_data[0], 0.0)
                self.curve.setData(self.x_data, self.history)
                if overflows:
                    self.capture_status.setText(
                        f"Captured {len(samples):,} samples; {overflows} DMA buffer overflow(s)"
                    )
                else:
                    self.capture_status.setText(
                        f"Captured {len(samples):,} samples; no DMA overflow"
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

if __name__ == "__main__":
    t = threading.Thread(target=udp_worker, daemon=True)
    t.start()
    capture_thread = threading.Thread(target=tcp_capture_worker, daemon=True)
    capture_thread.start()

    app = QtWidgets.QApplication([])
    window = LivePlot()
    window.show()
    app.exec()
