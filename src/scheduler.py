import schedule
import time
import subprocess
import logging
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SCRIPTS_DIR = os.path.join(BASE_DIR)
LOGS_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "logs"))

# Ensure logs directory exists
os.makedirs(LOGS_DIR, exist_ok=True)

# Paths to scripts
FIRST_SCRIPT = os.path.join(SCRIPTS_DIR, "analytics_script.py")
SECOND_SCRIPT = os.path.join(SCRIPTS_DIR, "best_selling_script.py")

# Paths to log files
CRON_LOG_FILE = os.path.join(LOGS_DIR, "scheduler.log")

# Configure logging
logging.basicConfig(filename=CRON_LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def run_script(script_path):
    try:
        logging.info(f"Running script: {script_path}")
        result = subprocess.run(
            ["python", script_path], capture_output=True, text=True)

        if result.stdout:
            logging.info(f"Output from {script_path}:\n{result.stdout}")

        if result.stderr:
            logging.error(f"Error from {script_path}:\n{result.stderr}")

        logging.info(f"Successfully executed: {script_path}")

    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to run {script_path}: {e}")


# Schedule tasks (run once a day)
schedule.every().day.at("15:22").do(run_script, script_path=FIRST_SCRIPT)
schedule.every().day.at("15:23").do(run_script, script_path=SECOND_SCRIPT)

logging.info("Scheduler started. Waiting for the next scheduled job...")

# Keep the scheduler running indefinitely
while True:
    schedule.run_pending()
    time.sleep(60)
