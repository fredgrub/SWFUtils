import re
import pandas as pd
from datetime import datetime
from .workload import Workload

class Reader:
    def __init__(self, filepath):
        self.filepath = filepath

    def read(self):
        # Define column names (job attributes)
        column_names = ["job_number", "submit_time", "wait_time", "run_time", "num_processors", 
                        "avg_cpu_time_used", "used_memory", "req_num_processors", "req_time", 
                        "req_memory", "status", "user_id", "group_id", "exec_number", 
                        "queue_number", "partition_number", "preceding_job_number", 
                        "think_time_from_preceding_job"]

        # Create a Workload object to store the data
        workload = Workload()

        # Parse some useful comments
        workload.meta = self.parse_comments()

        # Read the SWF file into a DataFrame
        workload.jobs = pd.read_csv(self.filepath, comment=';', delim_whitespace=True, 
                                    header=None, names=column_names)

        return workload
    
    def parse_comments(self):
        comments_data = {}

        # Define the patterns to look for
        patterns = {
            "UnixStartTime": re.compile(r"^;\s*UnixStartTime:\s*(\d+)\s*$"),
            "TimeZoneString": re.compile(r"^;\s*TimeZoneString:\s*([\w\/]+)\s*$"),
            "MaxJobs": re.compile(r"^;\s*MaxJobs:\s*(\d+)\s*$"),
            "MaxProcs": re.compile(r"^;\s*MaxProcs:\s*(\d+)\s*$"),
            "MaxNodes": re.compile(r"^;\s*MaxNodes:\s*(\d+)\s*$"),
        }

        # Map keys to their appropriate parser function
        parsers = {
            "UnixStartTime": lambda timestamp: datetime.fromtimestamp(int(timestamp)),
            "TimeZoneString": str,
            "MaxJobs": int,
            "MaxProcs": int,
            "MaxNodes": int,
        }

        with open(self.filepath, 'r') as f:
            for line in f:
                if line.startswith(';'):
                    for key, pattern in patterns.items():
                        match = pattern.match(line)
                        if match:
                            # Apply the appropriate parser function
                            comments_data[key] = parsers[key](match.group(1))
                else:
                    break

        return comments_data
