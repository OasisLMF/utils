"""
This file compresses the footprint file.
"""
from subprocess import Popen


def compress_footprint_file(static_path: str, intensity_bins: int) -> None:
    """
    Compresses the footprint file to a compressed file.

    :param static_path: (str) the path to the static file
    :param intensity_bins: (int) the number of intensity bins
    :return: None
    """
    command: str = f"footprinttocsv -b {static_path}/footprint.bin -x {static_path}/footprint.idx | " \
                   f"footprinttobin -z -u -b {static_path}/footprint.bin.z -x {static_path}/footprint.idx.z " \
                   f"-i {intensity_bins}"
    compression_process = Popen(command, shell=True)
    compression_process.wait()
