from .helpers import *

from multiprocessing import Lock


class ContextProgress:
    def __init__(
        self,
        context,
        metadata=None,
        futures=set(),
        errors=[],
        files_to_process=set(),
        cancelled=False,
    ):
        self.context = context
        self.metadata = metadata
        self.futures = futures
        self.errors = errors
        self.files_to_process = files_to_process
        self.cancelled = cancelled


class ProgressManager:
    def __init__(self):
        self._total_files = 0
        self._processed_files = 0
        self._failed_files = 0
        self._processed_files_lock = Lock()
        self._failed_files_lock = Lock()
        self._total_files_lock = Lock()
        self._register_lock = Lock()

    def register_processed_file(self):
        self._processed_files_lock.acquire()
        self._failed_files_lock.acquire()
        self._total_files_lock.acquire()
        self._register_lock.acquire()

        if self._processed_files + self._failed_files == self._total_files:
            self._register_lock.release()
            raise ProgressError(
                "All files have been registered as either processed or failed"
            )
        self._processed_files += 1
        processed_files = self._processed_files
        failed_files = self._failed_files

        self._processed_files_lock.release()
        self._failed_files_lock.release()
        self._total_files_lock.release()
        self._register_lock.release()
        return (processed_files, failed_files)

    def register_failed_file(self):
        self._processed_files_lock.acquire()
        self._failed_files_lock.acquire()
        self._total_files_lock.acquire()
        self._register_lock.acquire()

        if self._processed_files + self._failed_files == self._total_files:
            self._register_lock.release()
            raise ProgressError(
                "All files have been registered as either processed or failed"
            )
        self._failed_files += 1
        processed_files = self._processed_files
        failed_files = self._failed_files

        self._processed_files_lock.release()
        self._failed_files_lock.release()
        self._total_files_lock.release()
        self._register_lock.release()
        return (processed_files, failed_files)

    def set_total_files(self, total_files):
        self._total_files_lock.acquire()
        if self._total_files != 0:
            self._total_files_lock.release()
            raise ProgressError("Total files already set")
        self._total_files = total_files
        self._total_files_lock.release()

    def get_total_files(self):
        self._total_files_lock.acquire()
        total_files = self._total_files
        self._total_files_lock.release()
        return total_files

    def increment_total_files(self):
        self._total_files_lock.acquire()
        self._total_files = self._total_files + 1
        self._total_files_lock.release()

    def set_processed_files(self, processed_files):
        self._processed_files_lock.acquire()
        self._processed_files = processed_files
        self._processed_files_lock.release()

    def get_processed_files(self):
        self._processed_files_lock.acquire()
        processed_files = self._processed_files
        self._processed_files_lock.release()
        return processed_files

    def set_failed_files(self, failed_files):
        self._failed_files_lock.acquire()
        self._failed_files = failed_files
        self._failed_files_lock.release()

    def get_failed_files(self):
        self._failed_files_lock.acquire()
        failed_files = self._failed_files
        self._failed_files_lock.release()
        return failed_files


class ProgressError(Exception):
    """Exception raised for errors during progress manager operations

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message=None):
        if message is None:
            message = "An error occurred in the progress manager"

        super().__init__(message)
