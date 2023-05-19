import os
from datetime import datetime
from SWFUtils.reader import Reader

def test_reader():
    test_file_path = os.path.join(os.path.dirname(__file__), "data/dummy_workload.swf")
    reader = Reader(test_file_path)

    expected_meta = {
        'UnixStartTime': datetime.fromtimestamp(1609459200),
        'TimeZoneString': 'Europe/London',
        'MaxJobs': 15,
        'MaxProcs': 4,
        'MaxNodes': 4,
    }

    comments = reader.parse_comments()
    assert comments == expected_meta

    workload = reader.read()
    assert workload.meta == expected_meta
    assert workload.jobs.shape == (expected_meta['MaxJobs'], 18)