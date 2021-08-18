import math
from multiprocessing import cpu_count

class Parallel:
    def __init__(self, input_files, file_size_limit, gtfs_db):
        self.input_files = input_files
        self.file_size_limit = file_size_limit
        self.gtfs_db = gtfs_db


def create_workers(input_files, worker_cnt=None, gtfs_db=None, file_size_limit=1000):
    """Create workers for multiprocessing"""

    # Distribute the process into all cores
    if worker_cnt is not None and isinstance(worker_cnt, int):
        core_cnt=worker_cnt
    elif worker_cnt is None:
        core_cnt = cpu_count()
    else:
        assert isinstance(worker_cnt, int), "The number of workers should be passed as an integer value."

    # File count
    file_cnt = len(input_files)

    # Batch size
    batch_size = math.ceil(file_cnt / core_cnt)

    # Create journey workers
    workers = []
    start_i = 0
    end_i = batch_size

    for i in range(0, core_cnt):
        # On the last iteration ensure that all the rest will be added
        if i == core_cnt - 1:
            # Slice the list
            selection = input_files[start_i:]
        else:
            # Slice the list
            selection = input_files[start_i:end_i]

        workers.append(Parallel(input_files=selection, file_size_limit=file_size_limit,
                                gtfs_db=gtfs_db))

        # Update indices
        start_i += batch_size
        end_i += batch_size

    return workers
