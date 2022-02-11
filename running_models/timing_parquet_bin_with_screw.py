"""
This script is for calculating the difference in time and memory consumption between modelpy reading binary files
and modelpy with parquet files. This script should be run in the same director as the model data.
"""
import time

from screw_driver.model_runs.components.command_run import CommandRun
from screw_driver.model_runs.components.eve_component import Eve
from screw_driver.model_runs.components.file_stash_manager import ModelRunFileManager
from screw_driver.model_runs.components.memory_profiler import MemoryProfiler


if __name__ == "__main__":
    # define the parameters
    file_manager = ModelRunFileManager(static_path="./static/")
    file_manager.move_to_stash(file_name="footprint.parquet", file=False)
    file_manager.move_to_stash(file_name="footprint.csv")
    file_manager.move_to_stash(file_name="footprint.bin.z")
    file_manager.move_to_stash(file_name="footprint.idx.z")

    eve = Eve(total_processes=4)
    commands = [
        f"{eve} | modelpy --ignore-file-type parquet z csv > /dev/null",
        f"{eve} | modelpy --ignore-file-type parquet z csv > /dev/null",
        f"{eve} | modelpy --ignore-file-type parquet z csv > /dev/null",
        f"{eve} | modelpy --ignore-file-type parquet z csv > /dev/null"
    ]
    command_runner = CommandRun(input_commands=commands)

    start = time.time()
    # setting off the 4 modelpy processes for binary files
    command_runner.fire()

    # pass the process IDs into the memory profiler to track the memory usage
    memory_profiler = MemoryProfiler(pids=command_runner.pids)
    memory_profiler.start()

    # block the script to wait for the model processes to finish
    command_runner.wait()
    finish = time.time()

    # stop the memory profiler
    MemoryProfiler.stop_profiler()
    memory_profiler.join()

    print(f"the time with bin files is: {finish - start}")

    # loads the data that the memory profiler has written
    data = MemoryProfiler.load_memory_data()

    # loop through and process IDs and print out the peak memory usage
    for pid in command_runner.pids:
        print(max(data[pid]))

    # remove the flag and the written memory data
    MemoryProfiler.cleanup()

    file_manager.move_to_stash(file_name="footprint.bin")
    file_manager.move_to_stash(file_name="footprint.idx")
    file_manager.get_from_stash(file_name="footprint.parquet", file=False)

    commands = [
        "eve 1 4 | modelpy --ignore-file-type z csv > /dev/null",
        "eve 2 4 | modelpy --ignore-file-type z csv > /dev/null",
        "eve 3 4 | modelpy --ignore-file-type z csv > /dev/null",
        "eve 4 4 | modelpy --ignore-file-type z csv > /dev/null"
    ]
    command_runner = CommandRun(input_commands=commands)

    # run the python model processes reading parquet files
    start = time.time()
    command_runner.fire()

    # pass the process IDs to the new MemoryProfiler instance
    memory_profiler = MemoryProfiler(pids=command_runner.pids)
    memory_profiler.start()

    # block the script until the python model processes have finished
    command_runner.wait()
    finish = time.time()

    # stop the memory profiler
    MemoryProfiler.stop_profiler()
    memory_profiler.join()

    print(f"the time with parquet is: {finish - start}")

    # loads the data that the memory profiler has written
    data = MemoryProfiler.load_memory_data()

    # loop through and process IDs and print out the peak memory usage
    for pid in command_runner.pids:
        print(max(data[pid]))

    # remove the flag and the written memory data
    MemoryProfiler.cleanup()

    file_manager.get_from_stash(file_name="footprint.csv")
    file_manager.get_from_stash(file_name="footprint.bin.z")
    file_manager.get_from_stash(file_name="footprint.idx.z")
    file_manager.get_from_stash(file_name="footprint.bin")
    file_manager.get_from_stash(file_name="footprint.idx")


