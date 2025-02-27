import subprocess
import sys
import signal


# run setup_db.py to initialize the database
# subprocess.Popen([sys.executable, "setup_db.py"]).wait()

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
    print("Terminated by user")