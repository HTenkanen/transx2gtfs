import math
from multiprocessing import cpu_count


class Parallel:
    def __init__(self, input_files, file_size_limit, gtfs_db):
        self.input_files = input_files
        self.file_size_limit = file_size_limit
        self.gtfs_db = gtfs_db


def create_workers(input_files, worker_cnt=None, gtfs_db=None, file_size_limit=1000):
    """Create workers for multiprocessing (at most one per input file)"""

    # Distribute the process into all cores
    if worker_cnt is None:
        if cpu_count() == 1:
            core_cnt = cpu_count()
        else:
            core_cnt = cpu_count() - 1
    elif isinstance(worker_cnt, int) and not isinstance(worker_cnt, bool):
        core_cnt = worker_cnt
    else:
        raise TypeError("The number of workers should be passed as an integer value.")

    if core_cnt < 1:
        raise ValueError("The number of workers should be at least 1.")

    # File count; never more workers than files
    file_cnt = len(input_files)
    core_cnt = max(1, min(core_cnt, file_cnt))

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

        if len(selection) > 0:
            workers.append(
                Parallel(
                    input_files=selection,
                    file_size_limit=file_size_limit,
                    gtfs_db=gtfs_db,
                )
            )

        # Update indices
        start_i += batch_size
        end_i += batch_size

    return workers
