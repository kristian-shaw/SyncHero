from .helpers import *

# from .context import InitializationError, Initializable

from concurrent.futures import ThreadPoolExecutor
from enum import Enum


class ResultStatus(Enum):
    DONE = 0
    EXTRACT_NEEDED = 1
    NORMALIZE_AUDIO_NEEDED = 2
    DOWNLOAD_FAILED = 3
    EXTRACT_FAILED = 4
    NORMALIZE_AUDIO_FAILED = 5


class ProcessType(Enum):
    DOWNLOAD = 0
    EXTRACT = 1
    DELETE = 2


class ProcessResult:
    def __init__(self, status, context, root_context=None, error=None):
        self.status = status
        self.context = context
        self.root_context = root_context
        self.error = error


class ProcessManager:
    def __init__(
        self,
        download_workers_per_remote,
        extract_workers,
        delete_workers,
        source_remote_name_map,
    ):
        self._download_pools = dict()
        for remote_name, download_workers in download_workers_per_remote.items():
            self._download_pools[remote_name] = ThreadPoolExecutor(
                max_workers=download_workers
            )
        self._extract_pool = ThreadPoolExecutor(max_workers=extract_workers)
        self._delete_pool = ThreadPoolExecutor(max_workers=delete_workers)
        self._exit_pool = ThreadPoolExecutor(max_workers=1)
        self._exit_future = None
        self._source_remote_name_map = source_remote_name_map
        self._download_futures = set()
        self._extract_futures = set()
        self._delete_futures = set()

    def get_download_pool(self, remote_name):
        return self._download_pools[remote_name]

    def get_download_pools(self):
        return list(self._download_pools.values())

    def get_extract_pool(self):
        return self._extract_pool

    def get_delete_pool(self):
        return self._delete_pool

    def get_exit_pool(self):
        return self._exit_pool

    def submit_download_task(self, context, root_context, task, *args):
        future = self._download_pools[
            self._source_remote_name_map[context.source_name]
        ].submit(task, *args)
        future.process_type = ProcessType.DOWNLOAD
        future.context = context
        future.root_context = root_context
        self._download_futures.add(future)
        return future

    def submit_extract_task(self, context, root_context, task, *args):
        future = self._extract_pool.submit(task, *args)
        future.process_type = ProcessType.EXTRACT
        future.context = context
        future.root_context = root_context
        self._extract_futures.add(future)
        return future

    def submit_delete_task(self, context, root_context, task, *args):
        future = self._delete_pool.submit(task, *args)
        future.process_type = ProcessType.DELETE
        future.context = context
        future.root_context = root_context
        self._delete_futures.add(future)
        return future

    def submit_exit_task(self, task, *args):
        if self._exit_future is None:
            future = self._exit_pool.submit(task, *args)
            self._exit_future = future
            return future
        else:
            return None

    def get_download_futures(self):
        return self._download_futures

    def get_extract_futures(self):
        return self._extract_futures

    def get_delete_futures(self):
        return self._delete_futures

    def get_exit_future(self):
        return self._exit_future

    def get_futures(self):
        return self._download_futures | self._extract_futures | self._delete_futures

    def remove_future(self, future):
        match future.process_type:
            case ProcessType.DOWNLOAD:
                self._download_futures.discard(future)
            case ProcessType.EXTRACT:
                self._extract_futures.discard(future)
            case ProcessType.DELETE:
                self._delete_futures.discard(future)
