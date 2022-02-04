"""
This script is for managing files around models to ensure that the right files are present for a model run.
"""
import os
from subprocess import Popen


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
