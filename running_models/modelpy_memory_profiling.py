"""
This script is for running multiple
"""
import os
import pickle
import time
from multiprocessing import Process
from subprocess import Popen
from typing import List, Dict

import psutil


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
    start = time.time()
    # setting off the 4 modelpy processes
    norm_one = Popen("eve 1 4 | modelpy > /dev/null", shell=True)
    norm_two = Popen("eve 2 4 | modelpy > /dev/null", shell=True)
    norm_three = Popen("eve 3 4 | modelpy > /dev/null", shell=True)
    norm_four = Popen("eve 4 4 | modelpy > /dev/null", shell=True)

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
    print(f"the time without server is: {finish - start}")

    # loads the data that the memory profiler has written
    with open("./data.pickle", "rb") as file:
        data = pickle.load(file)

    # loop through and process IDs and print out the peak memory usage
    for pid in pids:
        print(max(data[pid]))

    # remove the flag and the written memory data
    os.remove("./data.pickle")
    os.remove("./flag.txt")

    # run the whole process again but with the data server
    server_process = Popen("servedata ./static/ 4", shell=True)

    # run the python model processes talking to the server
    start = time.time()
    serve_one = Popen("eve 1 4 | modelpy --data-server > /dev/null", shell=True)
    serve_two = Popen("eve 2 4 | modelpy --data-server > /dev/null", shell=True)
    serve_three = Popen("eve 3 4 | modelpy --data-server > /dev/null", shell=True)
    serve_four = Popen("eve 4 4 | modelpy --data-server > /dev/null", shell=True)

    # pass the process IDs to the new MemoryProfiler instance
    pids = [server_process.pid, serve_one.pid, serve_two.pid, serve_three.pid, serve_four.pid]
    test = MemoryProfiler(pids=pids)
    test.start()

    # block the script until the python model processes have finished
    serve_one.wait()
    serve_two.wait()
    serve_three.wait()
    serve_four.wait()

    # write DONE to the flag.txt to tell the memory profiler to stop and write the data
    with open("./flag.txt", "w") as file:
        file.write("DONE")

    # block the script until the memory profiler has written the data to the data.pickle
    test.join()
    server_process.kill()

    finish = time.time()
    print(f"the time with server is: {finish - start}")

    # loads the data that the memory profiler has written
    with open("./data.pickle", "rb") as file:
        data = pickle.load(file)

    # loop through and process IDs and print out the peak memory usage
    for pid in pids:
        print(max(data[pid]))

    # remove the flag and the written memory data
    os.remove("./data.pickle")
    os.remove("./flag.txt")
