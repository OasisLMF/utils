"""
This script is for calculating the difference in time and memory consumption between modelpy reading binary files
and modelpy with parquet files. This script should be run in the same director as the model data.
"""
import os
import pickle
import time
from multiprocessing import Process
from subprocess import Popen
from typing import List, Dict

import psutil


class ModelRunFileManager:
    """
    This class is responsible for managing files in the static directory when running a model.

    Attributes:
        static_path (str): the path to the static folder where the data files are housed
    """
    def __init__(self, static_path: str) -> None:
        """
        The constructor of the ModelRunFileManager.

        :param static_path: (str) the path to the static folder where the data files are housed
        """
        self.static_path: str = static_path
        self._establish_file_stash()

    def _establish_file_stash(self) -> None:
        """
        Creates a stash directory if there isn't one

        :return: None
        """
        if not os.path.isdir(self.stash_path):
            os.mkdir(self.stash_path)

    def move_to_stash(self, file_name: str, file: bool = True) -> None:
        """
        Moves the data file from the static directory to the stash directory.

        :param file_name: (str) the file name or directory than needs to be moved
        :param file: (bool) if set to False moves a directory as opposed to a file
        :return: None
        """
        static_file_path: str = str(os.path.join(self.static_path, file_name))
        stash_file_path: str = str(os.path.join(self.stash_path, file_name))

        if os.path.isfile(static_file_path) and file is True:
            move_process = Popen(f"mv {static_file_path} {stash_file_path}", shell=True)
            move_process.wait()
        elif os.path.isdir(static_file_path) and file is False:
            copy_process = Popen(f"cp -r {static_file_path} {stash_file_path}", shell=True)
            copy_process.wait()
            delete_process = Popen(f"rm -r {static_file_path}", shell=True)
            delete_process.wait()

    def get_from_stash(self, file_name: str, file: bool = True) -> None:
        """
        Moves a data file from the stash directory to the static directory.

        :param file_name: (str) the file name or directory than needs to be moved
        :param file: (bool) if set to False moves a directory as opposed to a file
        :return: None
        """
        static_file_path: str = str(os.path.join(self.static_path, file_name))
        stash_file_path: str = str(os.path.join(self.stash_path, file_name))

        if os.path.isfile(stash_file_path) and file is True:
            move_process = Popen(f"mv {stash_file_path} {static_file_path}", shell=True)
            move_process.wait()
        elif os.path.isdir(stash_file_path) and file is False:
            copy_process = Popen(f"cp -r {stash_file_path} {static_file_path}", shell=True)
            copy_process.wait()
            delete_process = Popen(f"rm -r {stash_file_path}", shell=True)
            delete_process.wait()

    @property
    def stash_path(self) -> str:
        return str(os.path.join(self.static_path, "stash/"))


class MemoryProfiler(Process):
    """
    This class is responsible for getting and storing the memory of the processes or interest.

    Attributes:
        pids (List[int]): a list of the process IDs that are to be monitored
        report (Dict[int, List[int]]): key => pid, value => a list of memory used over time for the process
    """
    def __init__(self, pids: List[int]) -> None:
        """
        The constructor for the MemoryProfiler class.

        Args:
            pids: (List[int]) a list of the process IDs that are to be monitored
        """
        super().__init__()
        self.pids: List[int] = pids
        self.report: Dict[int, List[int]] = dict()

    @staticmethod
    def _get_process_data(pid: int) -> int:
        """
        Gets memory usage at the current time for a process.

        Args:
            pid: (int) the ID of the process being checked on
        Returns: (int) the memory usage of the process
        """
        return psutil.Process(pid).memory_info()[0]

    def setup_report(self) -> None:
        """
        Initialises the self.report with the self.pids as keys.

        Returns: None
        """
        for pid in self.pids:
            self.report[pid] = []

    def run(self) -> None:
        """
        Runs when the MemoryProfiler process starts (overwritten from the super().Process class).

        Returns: None
        """
        self.setup_report()

        while True:
            if os.path.isfile("./flag.txt"):
                break
            for pid in self.pids:
                try:
                    memory_usage: int = self._get_process_data(pid=pid)
                    self.report[pid].append(memory_usage)
                except psutil.NoSuchProcess:
                    pass

        with open("./data.pickle", "wb") as file:
            pickle.dump(self.report, file)


if __name__ == "__main__":
    file_manager = ModelRunFileManager(static_path="./static/")
    file_manager.move_to_stash(file_name="footprint.parquet", file=False)
    file_manager.move_to_stash(file_name="footprint.csv")
    file_manager.move_to_stash(file_name="footprint.bin.z")
    file_manager.move_to_stash(file_name="footprint.idx.z")

    start = time.time()
    # setting off the 4 modelpy processes for binary files
    norm_one = Popen("eve 1 4 | modelpy --ignore-file-type parquet z csv > /dev/null", shell=True)
    norm_two = Popen("eve 2 4 | modelpy --ignore-file-type parquet z csv > /dev/null", shell=True)
    norm_three = Popen("eve 3 4 | modelpy --ignore-file-type parquet z csv > /dev/null", shell=True)
    norm_four = Popen("eve 4 4 | modelpy --ignore-file-type parquet z csv > /dev/null", shell=True)

    # pass the process IDs into the memory profiler to track the memory usage
    pids = [norm_one.pid, norm_two.pid, norm_three.pid, norm_four.pid]
    test = MemoryProfiler(pids=pids)
    test.start()

    # block the script to wait for the model processes to finish
    norm_one.wait()
    norm_two.wait()
    norm_three.wait()
    norm_four.wait()
    finish = time.time()

    # write DONE to flag.txt to tell the MemoryProfiler to stop
    with open("./flag.txt", "w") as file:
        file.write("DONE")

    # block the script until the MemoryProfiler has finished
    test.join()
    print(f"the time with bin files is: {finish - start}")

    # loads the data that the memory profiler has written
    with open("./data.pickle", "rb") as file:
        data = pickle.load(file)

    # loop through and process IDs and print out the peak memory usage
    for pid in pids:
        print(max(data[pid]))

    # remove the flag and the written memory data
    os.remove("./data.pickle")
    os.remove("./flag.txt")

    file_manager.move_to_stash(file_name="footprint.bin")
    file_manager.move_to_stash(file_name="footprint.idx")
    file_manager.get_from_stash(file_name="footprint.parquet", file=False)

    # run the python model processes reading parquet files
    start = time.time()
    parq_one = Popen("eve 1 4 | modelpy --ignore-file-type z csv > /dev/null", shell=True)
    parq_two = Popen("eve 2 4 | modelpy --ignore-file-type z csv > /dev/null", shell=True)
    parq_three = Popen("eve 3 4 | modelpy --ignore-file-type z csv > /dev/null", shell=True)
    parq_four = Popen("eve 4 4 | modelpy --ignore-file-type z csv > /dev/null", shell=True)

    # pass the process IDs to the new MemoryProfiler instance
    pids = [parq_one.pid, parq_two.pid, parq_three.pid, parq_four.pid]
    test = MemoryProfiler(pids=pids)
    test.start()

    # block the script until the python model processes have finished
    parq_one.wait()
    parq_two.wait()
    parq_three.wait()
    parq_four.wait()

    # write DONE to the flag.txt to tell the memory profiler to stop and write the data
    with open("./flag.txt", "w") as file:
        file.write("DONE")

    # block the script until the memory profiler has written the data to the data.pickle
    test.join()

    finish = time.time()
    print(f"the time with parquet is: {finish - start}")

    # loads the data that the memory profiler has written
    with open("./data.pickle", "rb") as file:
        data = pickle.load(file)

    # loop through and process IDs and print out the peak memory usage
    for pid in pids:
        print(max(data[pid]))

    # remove the flag and the written memory data
    os.remove("./data.pickle")
    os.remove("./flag.txt")

    file_manager.get_from_stash(file_name="footprint.csv")
    file_manager.get_from_stash(file_name="footprint.bin.z")
    file_manager.get_from_stash(file_name="footprint.idx.z")
    file_manager.get_from_stash(file_name="footprint.bin")
    file_manager.get_from_stash(file_name="footprint.idx")


