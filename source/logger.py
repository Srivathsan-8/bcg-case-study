import logging

class LogCapture:

    """Log Capture is to capture the happening's of this application and logs in the mentioned file location and it has two methods for success and error and based on the requirement it appends the file.
    """
    def __init__(self):
        # Define the log file path
        self.log_file_path = "Log/bcg_case_study.log"
        try:
            # Configure logging
            logging.basicConfig(
                filename=self.log_file_path,
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
            )
        except Exception as e:
            print(f"Error setting up logging: {e}")

    def log_success(self, msg):
        logging.info(msg)

    def log_error(self, msg):
        logging.error(msg)
