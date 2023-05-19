import pandas as pd

class Workload:
    """
    A class to represent a workload of jobs.

    Attributes
    ----------
    meta : dict
        A dictionary containing specific metadata about the workload.
    jobs : pandas.DataFrame
        A DataFrame containing the job data.
    """

    def __init__(self):
        """
        Initializes a new Workload object.
        """
        self.meta = {}
        self.jobs = pd.DataFrame()