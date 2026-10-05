import socket
import struct
import threading
import queue
import numpy as np
import pyqtgraph as pg
from PyQt6 import QtCore, QtWidgets

PORT = 4444
TOTAL_SAMPLES = 1024
SAMPLES_PER_PACKET = 512
NUM_PARTS = TOTAL_SAMPLES // SAMPLES_PER_PACKET

frame_queue = queue.Queue(maxsize=5)

def udp_worker():
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
        self.resize(900, 500)
        self.plot_widget = pg.PlotWidget()
        self.setCentralWidget(self.plot_widget)
        self.plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self.plot_widget.setYRange(0, 4096)
        self.plot_widget.setXRange(0, TOTAL_SAMPLES)
        
        self.curve = self.plot_widget.plot(pen=pg.mkPen(color="#00ffff", width=1.5))
        
        self.timer = QtCore.QTimer()
        self.timer.timeout.connect(self.update_plot)
        self.timer.start(1000//60)

    def update_plot(self):
        latest = None
        while not frame_queue.empty():
            latest = frame_queue.get_nowait()

        if latest is not None:
            self.curve.setData(latest)

if __name__ == "__main__":
    t = threading.Thread(target=udp_worker, daemon=True)
    t.start()

    app = QtWidgets.QApplication([])
    window = LivePlot()
    window.show()
    app.exec()
