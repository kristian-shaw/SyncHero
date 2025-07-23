from .global_config import GloballyConfigured
from .helpers import *

from multiprocessing import Lock
from pathlib import Path
from typing import Any

import dataclasses


@dataclasses.dataclass
class Context:
    source_name: str | None = None
    file_path: Path | None = None

    def as_path(self, include_source: bool = True) -> Path | None:
        if include_source:
            if self.source_name is None or self.file_path is None:
                return None
            else:
                return Path(self.source_name, self.file_path)
        else:
            return self.file_path


class ContextPool:
    def __init__(self) -> None:
        self.lock = Lock()
        self.active_contexts = set()


_context_pools: dict[str, ContextPool] = {"default": ContextPool()}


class Contextual(GloballyConfigured):

    _context_pool_names: set[str] = {"default"}

    def __init__(self) -> None:
        self.context = Context()
        self.lock_stack = []

    @classmethod
    def configure(cls, context_pool_names: set[str]) -> None:
        cls._context_pool_names = context_pool_names
        for pool_name in context_pool_names:
            if pool_name not in _context_pools:
                _context_pools[pool_name] = ContextPool()
        super().configure()

    @classmethod
    def lock_context_changes(cls) -> None:
        for pool_name in cls._context_pool_names:
            _context_pools[pool_name].lock.acquire()

    @classmethod
    def unlock_context_changes(cls) -> None:
        for pool_name in cls._context_pool_names:
            _context_pools[pool_name].lock.release()

    def get_context_source_name(self) -> str:
        return self.context.source_name

    def set_context_source_name(self, source_name: str) -> None:
        if not isinstance(source_name, str):
            raise InvalidContextSubmissionError(
                "source_name",
                source_name,
                f"Invalid value submitted for context source_name was not of type str: {safe_str(source_name)}",
            )
        else:
            self.lock_context_changes()
            try:
                self.context.file_path = None
                self.context.source_name = source_name
            finally:
                self.unlock_context_changes()

    def get_context_file_path(self) -> Path:
        return self.context.file_path

    def set_context_file_path(self, file_path: str) -> None:
        self.lock_context_changes()
        context = self.get_context()
        if context.source_name == None:
            self.unlock_context_changes()
            raise InvalidContextError(
                context.source_name,
                context.file_path,
                f"Context does not have source_name set",
            )
        elif not isinstance(file_path, str):
            self.unlock_context_changes()
            raise InvalidContextSubmissionError(
                "file_path",
                context.file_path,
                f"Invalid value submitted for context file_path was not of type str: {safe_str(file_path)}",
            )
        else:
            if self.context_is_set():
                for pool_name in self._context_pool_names:
                    _context_pools[pool_name].active_contexts.remove(
                        (context.as_path())
                    )
            self.context.file_path = file_path
            context = self.context
            target_pool_names = []
            for pool_name in self._context_pool_names:
                if str(context.as_path()) in _context_pools[pool_name].active_contexts:
                    self.context.file_path = None
                    self.unlock_context_changes()
                    raise InvalidContextSubmissionError(
                        context.source_name,
                        file_path,
                        f"Context is already in use. source_name: {context.source_name}, file_path: {safe_str(file_path)}",
                    )
                else:
                    target_pool_names.append(pool_name)
            for pool_name in target_pool_names:
                _context_pools[pool_name].active_contexts.add((context.as_path()))
            self.unlock_context_changes()

    def get_context(self) -> Context:
        return dataclasses.replace(self.context)  # Returns a copy

    def set_context(self, context: Context) -> None:
        context = dataclasses.replace(
            context
        )  # Create a copy not modifiable by anything else
        if not isinstance(context, Context):
            raise InvalidContextSubmissionError(
                "context",
                context,
                f"Invalid value submitted for context was not of type Context: {safe_str(context)}",
            )
        else:
            self.set_context_source_name(context.source_name)
            self.set_context_file_path(context.file_path)

    def context_is_set(self) -> bool:
        context = self.get_context()
        return context.file_path is not None

    def free_context(self) -> None:
        self.lock_context_changes()
        if self.context_is_set():
            for pool_name in self._context_pool_names:
                _context_pools[pool_name].active_contexts.remove(
                    (self.context.as_path())
                )
        self.context.file_path = None
        self.context.source_name = None
        self.unlock_context_changes()

    def raise_exception_if_context_not_set(self) -> None:
        if not self.context_is_set():
            context = self.get_context()
            raise InvalidContextError(
                context.source_name,
                context.file_path,
                f"Context is not set. source_name: {context.source_name}, file_path: {safe_str(context.file_path)}",
            )


class InvalidContextSubmissionError(Exception):
    """Exception raised for submission of invalid metadata

    Attributes:
        attribute_name -- the name of attribute in the context
        value          -- the value submitted for the attribute
        message        -- explanation of the error
    """

    def __init__(
        self, attribute_name: str, value: Any, message: str | None = None
    ) -> None:
        self.attribute_name = attribute_name
        self.value = value

        if message is None:
            message = f"Invalid value submitted for context attribute {safe_str(attribute_name)}: {safe_str(value)}"

        super().__init__(message)


class ContextualError(Exception):
    """Base Exception class for errors related to a Context

    Attributes:
        source_name -- the name of the source in the context
        file_path   -- the file path in the context
        message     -- explanation of the error
    """

    def __init__(
        self, source_name: str, file_path: str, message: str | None = None
    ) -> None:
        self.source_name = source_name
        self.file_path = file_path

        if message is None:
            message = f"A problem occured related to a Context. source_name: {source_name}, file_path: {file_path}"

        super().__init__(message)


class InvalidContextError(ContextualError):
    """Exception raised for invalid context

    Attributes:
        source_name -- the name of the source in the context
        file_path   -- the file path in the context
        message     -- explanation of the error
    """

    def __init__(
        self, source_name: str, file_path: str, message: str | None = None
    ) -> None:
        self.source_name = source_name
        self.file_path = file_path

        if message is None:
            message = f"Invalid context. source_name: {safe_str(source_name)}, file_path: {safe_str(file_path)}"

        super().__init__(source_name, file_path, message)
