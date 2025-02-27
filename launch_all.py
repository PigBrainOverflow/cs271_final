import subprocess
import sys
import signal


processes = [
    subprocess.Popen([sys.executable, "launch_router.py"]),
    subprocess.Popen([sys.executable, "launch_server.py", "--cluster", "0", "--member", "0"]),
    subprocess.Popen([sys.executable, "launch_server.py", "--cluster", "0", "--member", "1"]),
    subprocess.Popen([sys.executable, "launch_server.py", "--cluster", "0", "--member", "2"]),
]

print("Launched successfully")
# wait for all scripts to finish
try:
    for process in processes:
        process.wait()
except KeyboardInterrupt:
    processes[0].send_signal(signal.SIGINT)
    for process in processes:
        process.wait()
    print("Terminated by user")