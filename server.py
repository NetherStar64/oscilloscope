import socket
import struct
import threading
import queue
import time
import numpy as np
import pyqtgraph as pg
from PyQt6 import QtCore, QtWidgets

PORT = 4444
TOTAL_SAMPLES = 1024
SAMPLES_PER_PACKET = 512
NUM_PARTS = TOTAL_SAMPLES // SAMPLES_PER_PACKET

NOMINAL_ADC_SPS = 500_000

frame_queue = queue.Queue(maxsize=5)

stats_lock = threading.Lock()
rx_frame_count = 0
rx_byte_count = 0

def udp_worker():
    global rx_frame_count, rx_byte_count
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", PORT))
    frames = {}

    while True:
        packet, _ = sock.recvfrom(2048)
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

class LivePlot(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Oscilloscope")
        self.resize(950, 550)

        self.plot_widget = pg.PlotWidget()
        self.setCentralWidget(self.plot_widget)
        self.plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self.plot_widget.setYRange(0, 4096)
        self.plot_widget.setXRange(0, TOTAL_SAMPLES)
        self.curve = self.plot_widget.plot(pen=pg.mkPen(color="#00ffff", width=1.5))

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

    def update_plot(self):
        latest = None
        while not frame_queue.empty():
            latest = frame_queue.get_nowait()

        if latest is not None:
            self.curve.setData(latest)

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
            nominal_ksps = NOMINAL_ADC_SPS / 1000.0

            duty_cycle = (effective_sps / NOMINAL_ADC_SPS) * 100.0
            mbps = (d_bytes * 8) / (dt * 1e6)

            msg = (
                f"Sample Speed: {duty_cycle:.2f}% of {nominal_ksps:.0f} kSps  |  " \
                f"Rate: {effective_ksps:.1f} kS/s ({fps:.1f} FPS)  |  " \
                f"Network: {mbps:.2f} Mbps" \
            )

            self.status.showMessage(msg)
            
if __name__ == "__main__":
    t = threading.Thread(target=udp_worker, daemon=True)
    t.start()

    app = QtWidgets.QApplication([])
    window = LivePlot()
    window.show()
    app.exec()
