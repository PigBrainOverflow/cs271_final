import subprocess
import sys
import signal
import json


# run setup_db.py to initialize the database
# subprocess.Popen([sys.executable, "setup_db.py"]).wait()

with open("config.json") as f:
    config = json.load(f)
nservers_per_cluster = [len(cluster["members"]) for cluster in config["clusters"]]

processes = [subprocess.Popen([sys.executable, "launch_router.py"])]
processes += [
    subprocess.Popen([sys.executable, "launch_server.py", "--cluster", str(cluster), "--member", str(member)])
    for cluster in range(len(nservers_per_cluster))
    for member in range(nservers_per_cluster[cluster])
]

print("Launched successfully")
# wait for all scripts to finish
try:
    for process in processes:
        process.wait()
except KeyboardInterrupt:
    processes[0].send_signal(signal.SIGINT)
    print("Terminated by user")