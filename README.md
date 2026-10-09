# Oscilloscope

The Pico 2 W streams oscilloscope data to `server.py` over its native USB
serial connection. Connect the Pico's USB-C port to the computer running the
server. A Picoprobe USB connection is still usable for debugging, but its SWD
connection does not carry the Pico's data stream; keep the separate Pico-to-PC
USB cable connected as well. If the probe wiring supplies power to the Pico,
disconnect that power feed before connecting the Pico's USB-C port to avoid
back-powering the board.

Install the Python dependencies with `python -m pip install -r requirements.txt`
and run `python server.py`. The server detects a single Pico USB serial port
automatically. If more than one Pico is connected, select the intended COM port
before starting the server by setting `OSCILLOSCOPE_PORT` (for example,
`$env:OSCILLOSCOPE_PORT = "COM5"` in PowerShell).
