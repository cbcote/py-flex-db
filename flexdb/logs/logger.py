# Write a class model for logging modules in connectors

import logging
import os
import sys
from datetime import datetime

from flexdb import config


class Logger:
    def __init__(self, name, level=logging.INFO):
        """Initialize the logger class
        
        Args:
            name (str): Name of the logger
            level (logging.level): Level of the logger
        
        Returns:
            None
        
        Raises:
            None
        """
        self.name = name
        self.level = level
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(self.level)
        self.formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.log_path = config.LOG_PATH
        self.log_file = os.path.join(self.log_path, f"{self.name}.log")
        self.file_handler = logging.FileHandler(self.log_file)
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)

    def info(self, msg):
        """Log info messages
        
        Args:
            msg (str): Message to log
        """
        self.logger.info(msg)

    def error(self, msg):
        """Log error messages
        
        Args:
            msg (str): Message to log
        
        """
        self.logger.error(msg)

    def debug(self, msg):
        """Log debug messages
        
        Args:
            msg (str): Message to log
        """
        self.logger.debug(msg)
