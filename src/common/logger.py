import logging
import os

os.makedirs("logs", exist_ok=True)

log_instance = logging.getLogger("employment_jumpscare")
log_instance.setLevel(logging.INFO)

file_handler = logging.FileHandler("logs/app.log", mode="a")
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
))

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
))

log_instance.addHandler(file_handler)
log_instance.addHandler(console_handler)
