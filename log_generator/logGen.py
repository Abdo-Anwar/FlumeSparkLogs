import time
import random
from datetime import datetime

log_levels = ["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"]
components = ["AuthService", "UserService", "PaymentService", "OrderService"]

log_file_path = "/home/bigdata/genLog.log"

def generate_log_line():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level = random.choice(log_levels)
    component = random.choice(components)
    message = f"{component} responded in {random.randint(50, 500)}ms"
    return f"{timestamp} - {level} - {component} - {message}\n"

with open(log_file_path, "a") as log_file:
    while True:
        log_line = generate_log_line()
        log_file.write(log_line)
        log_file.flush()  # Flush to ensure Flume sees it immediately
        time.sleep(1)

