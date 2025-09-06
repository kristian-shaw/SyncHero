from .global_config import GloballyConfigured
from .helpers import *

from multiprocessing import Lock
from multiprocessing.synchronize import Lock as LockT
from pathlib import Path
from pydantic import ValidationError
from typing import Any, Self

import dataclasses


@dataclasses.dataclass
class Context:
    source_name: str | None = None
    file_path: Path | None = None

    def __hash__(self) -> int:
        return hash(str(self.source_name) + str(self.file_path))

    def as_path(self, include_source: bool = True) -> Path | None:
        if include_source:
            if self.source_name is None or self.file_path is None:
                return None
            else:
                return Path(self.source_name, self.file_path)
        else:
            return self.file_path

    @staticmethod
    def from_path(path: Path) -> Self:
        return Context(source_name=Path(path.parts[0]), file_path=Path(*path.parts[1:]))


class ContextPool:
    def __init__(self) -> None:
        self.lock = Lock()
        self.active_contexts = set()


_DEFAULT_POOL_NAME: str = "default"
_context_pools: dict[str, set[Context]] = {_DEFAULT_POOL_NAME: set[Context]()}
_context_lock: LockT = Lock()


class Contextual(GloballyConfigured):
    def __init__(self) -> None:
        self.context = Context()

    @classmethod
    def configure(cls, context_pool_name: str = _DEFAULT_POOL_NAME) -> None:
        global _DEFAULT_POOL_NAME, _context_pools
        cls._context_pool_name = context_pool_name
        if context_pool_name not in _context_pools.keys():
            _context_pools[context_pool_name] = set[Context]()
        super().configure()

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
            if self.context_is_set():
                self.free_context()
            self.context.source_name = source_name

    def get_context_file_path(self) -> Path:
        return self.context.file_path

    def set_context_file_path(self, file_path: Path) -> None:
        global _context_pools, _context_lock
        with _context_lock:
            context = self.get_context()
            if context.source_name is None:
                raise InvalidContextError(
                    context.source_name,
                    context.file_path,
                    f"Context does not have source_name set",
                )
            elif not isinstance(file_path, Path):
                raise InvalidContextSubmissionError(
                    "file_path",
                    context.file_path,
                    f"Invalid value submitted for context file_path was not of type Path: {safe_str(file_path)}",
                )
            else:
                if self.context_is_set():
                    _context_pools[self._context_pool_name].remove(self.context)
                if self.context in _context_pools[self._context_pool_name]:
                    self.context.file_path = None
                    raise InvalidContextSubmissionError(
                        context.source_name,
                        file_path,
                        f"Context is already in use. Context: {self.context.__dict__}",
                    )
                else:
                    self.context.file_path = file_path
                    _context_pools[self._context_pool_name].add(self.context)

    def get_context(self) -> Context:
        return dataclasses.replace(self.context)  # Returns a copy

    def set_context(self, context: Context) -> None:
        new_context = dataclasses.replace(
            context
        )  # Create a copy not modifiable by anything else
        if not isinstance(new_context, Context):
            raise InvalidContextSubmissionError(
                "context",
                new_context,
                f"Invalid value submitted for context was not of type Context: {safe_str(new_context)}",
            )
        else:
            self.set_context_source_name(new_context.source_name)
            self.set_context_file_path(new_context.file_path)

    def context_is_set(self) -> bool:
        context = self.get_context()
        return context.file_path is not None

    def free_context(self) -> None:
        global _context_pools, _context_lock
        with _context_lock:
            if self.context_is_set():
                _context_pools[self._context_pool_name].remove(self.context)
            self.context.file_path = None
            self.context.source_name = None

    def raise_exception_if_context_not_set(self) -> None:
        if not self.context_is_set():
            context = self.get_context()
            raise InvalidContextError(
                context.source_name,
                context.file_path,
                f"Context is not set. Context: {context.__dict__}",
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
        context -- the context related to the error
        message -- explanation of the error
    """

    def __init__(self, context: Context, message: str | None = None) -> None:
        self.context = context
        if message is None:
            message = f"A problem occured related to a Context: {context.__dict__}"

        super().__init__(message)


class InvalidContextError(ContextualError):
    """Exception raised for invalid context

    Attributes:
        context -- the context related to the error
        message -- explanation of the error
    """

    def __init__(self, context: Context, message: str | None = None) -> None:
        self.context = context
        if message is None:
            message = f"Invalid context. Context: {context.__dict__}"

        super().__init__(context, message)


class ContextualValidationError(Exception):
    """Exception that includes a Pydantic ValidationError as an attribute as well as a context
    By default the error message states the context first and then the message from the ValidationError

    Attributes:
        context          -- the context related to the error
        validation_error -- the validation error
        message          -- explanation of the error
    """

    def __init__(
        self,
        context: Context,
        validation_error: ValidationError,
        message: str | None = None,
    ) -> None:
        self.context = context
        self.validation_error = validation_error
        if message is None:
            message = f"ValidationError occured. Context: {context.__dict__}\n{validation_error}"

        super().__init__(message)
