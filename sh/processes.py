from .helpers import *
from .context import Context, ContextualError

# from .context import InitializationError, Initializable

from collections.abc import Callable
from typing import Any
from concurrent.futures import Future, ThreadPoolExecutor
from enum import Enum

import dataclasses


class ResultStatus(Enum):
    DONE = 0
    EXTRACT_NEEDED = 1
    DELETE_NEEDED = 2
    DOWNLOAD_FAILED = 3
    EXTRACT_FAILED = 4
    DELETE_FAILED = 5


class ProcessType(Enum):
    DOWNLOAD = 0
    EXTRACT = 1
    DELETE = 2


@dataclasses.dataclass
class ContextualFutureResult:
    context: Context
    status: ResultStatus
    error: Exception
    future_context: Context | None = None


@dataclasses.dataclass
class FutureInfo:
    future: Future
    process_type: ProcessType


class ProcessManager:
    def __init__(
        self,
        download_workers_per_remote: dict[str, int],
        extract_workers: int,
        delete_workers: int,
        source_remote_name_map: dict[str, str],
    ):
        self._source_remote_name_map = source_remote_name_map

        self._download_pools = dict[str, ThreadPoolExecutor]()
        for remote_name, download_workers in download_workers_per_remote.items():
            self._download_pools[remote_name] = ThreadPoolExecutor(
                max_workers=download_workers
            )
        self._download_futures = list[Future]()

        self._extract_pool = ThreadPoolExecutor(max_workers=extract_workers)
        self._extract_futures = list[Future]()

        self._delete_pool = ThreadPoolExecutor(max_workers=delete_workers)
        self._delete_futures = list[Future]()

        self._exit_pool = ThreadPoolExecutor(max_workers=1)
        self._exit_future: Future | None = None

        self._context_future_info_map = dict[Context, FutureInfo]()

    def get_download_pool(self, remote_name: str) -> ThreadPoolExecutor:
        return self._download_pools[remote_name]

    def get_download_pools(self) -> list[ThreadPoolExecutor]:
        return list(self._download_pools.values())

    def get_extract_pool(self) -> ThreadPoolExecutor:
        return self._extract_pool

    def get_delete_pool(self) -> ThreadPoolExecutor:
        return self._delete_pool

    def get_exit_pool(self) -> ThreadPoolExecutor:
        return self._exit_pool

    def submit_contextual_task(
        self, process_type: ProcessType, context: Context, task: Callable, *args: Any
    ) -> Future:
        if context in self._context_future_info_map.keys():
            raise FutureContextExistsError(context)
        match process_type:
            case ProcessType.DOWNLOAD:
                pool = self._download_pools[
                    self._source_remote_name_map[context.source_name]
                ]
                future_list = self._download_futures
            case ProcessType.EXTRACT:
                pool = self._extract_pool
                future_list = self._extract_futures
            case ProcessType.DELETE:
                pool = self._delete_pool
                future_list = self._delete_futures

        future = pool.submit(task, *args)
        future_list.append(future)
        self._context_future_info_map[context] = FutureInfo(future, process_type)

        return future

    def submit_download_task(
        self, context: Context, task: Callable, *args: Any
    ) -> Future:
        return self.submit_contextual_task(ProcessType.DOWNLOAD, context, task, *args)

    def submit_extract_task(
        self, context: Context, task: Callable, *args: Any
    ) -> Future:
        return self.submit_contextual_task(ProcessType.EXTRACT, context, task, *args)

    def submit_delete_task(
        self, context: Context, task: Callable, *args: Any
    ) -> Future:
        return self.submit_contextual_task(ProcessType.DELETE, context, task, *args)

    def submit_exit_task(self, task: Callable, *args: Any) -> Future | None:
        if self._exit_future is None:
            future = self._exit_pool.submit(task, *args)
            self._exit_future = future
            return future
        else:
            return None

    def get_download_futures(self) -> list[Future]:
        return self._download_futures

    def get_extract_futures(self) -> list[Future]:
        return self._extract_futures

    def get_delete_futures(self) -> list[Future]:
        return self._delete_futures

    def get_exit_future(self) -> Future:
        return self._exit_future

    def get_futures(self) -> list[Future]:
        return self._download_futures + self._extract_futures + self._delete_futures

    def get_context_for_future(self, future: Future) -> Context | None:
        for context, future_info in self._context_future_info_map.items():
            if future_info.future is future:
                return context
        raise UnknownFutureError(future)

    def get_info_for_future(self, future: Future) -> Context | None:
        for future_info in self._context_future_info_map.values():
            if future_info.future is future:
                return future_info
        raise UnknownFutureError(future)

    def remove_future(self, future: Future) -> None:
        context = self.get_context_for_future(future)
        future_info = self._context_future_info_map[context]

        del self._context_future_info_map[context]
        match future_info.process_type:
            case ProcessType.DOWNLOAD:
                self._download_futures.remove(future)
            case ProcessType.EXTRACT:
                self._extract_futures.remove(future)
            case ProcessType.DELETE:
                self._delete_futures.remove(future)


class UnknownFutureError(Exception):
    """Exception raised when info cannot be found for a Future

    Attributes:
        future  -- the Future no info was found for
        message -- explanation of the error
    """

    def __init__(self, future: Future, message: str | None = None) -> None:
        self.future = future
        if message is None:
            message = f"The submitted Future is not tracked by the ProcessManager."

        super().__init__(message)


class FutureContextExistsError(ContextualError):
    """Exception raised when a task is submitted for a Context a Future already exists for

    Attributes:
        context -- the Context a Future already exists for
        message -- explanation of the error
    """

    def __init__(self, context: Context, message: str | None = None) -> None:
        if message is None:
            message = (
                f"A Future already exists for the submitted Context: {context.__dict__}"
            )

        super().__init__(context, message)
