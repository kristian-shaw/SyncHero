from .helpers import *
from .context import Contextual

from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import nullcontext
from enum import Enum
from multiprocessing import Lock
from pathlib import Path
from pydantic import BaseModel, Field, ValidationError
from time import sleep, time
from typing import Any, Iterable

import json


class ContextError(Enum):
    UNKNOWN = 0
    CANCELLED = 1
    DOWNLOAD_FAILED = 2
    EXTRACT_FAILED = 3
    ARCHIVE_DELETION_FAILED = 4


class ContextFileType(Enum):
    UNKNOWN = 0
    ARCHIVE = 1


class ContextMetadata(BaseModel, validate_assignment=True):
    error_codes: set[ContextError] = Field(
        alias="a", default_factory=set
    )  # Codes (enum) for errors
    file_type: ContextFileType = Field(
        alias="b", default=ContextFileType.UNKNOWN
    )  # Code (enum) for the file type
    parent_path: Path = Field(
        alias="c", default_factory=Path
    )  # The path of the archive the file is a member of, or an empty string if not an archive member or a root archive
    remote_hash: str = Field(alias="d", default="")  # From remote storage API

    def __delitem__(self, item: str) -> None:
        delattr(self, item)

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)

    def __setitem__(self, item: str, value: Any) -> None:
        setattr(self, item, value)

    @classmethod
    def get_attribute_names(cls) -> list[str]:
        return list(dict(cls.model_json_schema(by_alias=False)["properties"]).keys())


class MetadataDict(BaseModel):
    version: str = Field(alias="v", default="1.0")
    metadata: dict[Path, ContextMetadata] = Field(alias="m", default_factory=dict)

    def __iter__(self) -> Iterable:
        return iter(self.metadata)

    def __delitem__(self, item: Path) -> None:
        del self.metadata[item]

    def __getitem__(self, item: Path) -> ContextMetadata:
        return self.metadata[item]

    def __setitem__(self, item: Path, value: ContextMetadata) -> None:
        self.metadata[item] = value


class MetadataManager(Contextual):
    def __init__(
        self, metadata_file_path: Path, metadata_flush_seconds: int, minimise_json: bool
    ):
        super().__init__()
        self._metadata_file_lock = Lock()
        self._enable_flush_metadata_process = False
        self._metadata_file_path = metadata_file_path
        self._metadata_flush_seconds = metadata_flush_seconds
        self._minimise_json = minimise_json
        if metadata_file_path.is_file():
            with open(metadata_file_path, "r") as metadata_file:
                self._metadata = MetadataDict.model_validate_json(metadata_file.read())
        else:
            self._metadata = MetadataDict()
            self.flush_metadata()

    @staticmethod
    def get_initialized_metadata() -> ContextMetadata:
        return ContextMetadata()

    @staticmethod
    def attribute_name_exists(attribute_name: str) -> bool:
        return attribute_name in ContextMetadata.get_attribute_names()

    def get_metadata_key(self) -> str:
        return self.context.as_path(include_source=True)

    def initialize_metadata(self, use_lock: bool = True) -> None:
        self.raise_exception_if_context_not_set()
        with self._metadata_file_lock if use_lock else nullcontext():
            self._metadata[self.get_metadata_key()] = self.get_initialized_metadata()

    def delete_metadata(self, use_lock: bool = True) -> None:
        self.raise_exception_if_context_not_set()
        with self._metadata_file_lock if use_lock else nullcontext():
            if self.metadata_exists():
                del self._metadata[self.get_metadata_key()]

    def get_attribute(self, attribute_name: str, use_lock: bool = True) -> Any:
        self.raise_exception_if_context_not_set()
        if not self.attribute_name_exists(attribute_name):
            raise InvalidMetadataFetchError(
                attribute_name, f"Metadata requested for invalid key: {attribute_name}"
            )
        with self._metadata_file_lock if use_lock else nullcontext():
            self.raise_exception_if_no_metadata()
            return self._metadata[self.get_metadata_key()][attribute_name]

    def set_attribute(
        self, attribute_name: str, attribute_value: Any, use_lock: bool = True
    ) -> None:
        self.raise_exception_if_context_not_set()
        if not self.attribute_name_exists(attribute_name):
            raise InvalidMetadataSubmissionError(
                attribute_name, f"Metadata submitted for invalid key: {attribute_name}"
            )
        with self._metadata_file_lock if use_lock else nullcontext():
            if not self.metadata_exists():
                self.initialize_metadata(use_lock=False)
            try:
                self._metadata[self.get_metadata_key()][
                    attribute_name
                ] = attribute_value
            except ValidationError as e:
                setattr(e, "context", self.get_context())
                raise e

    def get_error_codes(self) -> set[ContextError]:
        return self.get_attribute("error_codes")

    def set_error_codes(self, error_codes: set[ContextError]) -> None:
        self.set_attribute("error_codes", error_codes)

    def set_error_code_status(
        self, error_code: ContextError, status: bool, use_lock: bool = True
    ) -> None:
        with self._metadata_file_lock if use_lock else nullcontext():
            error_codes = set[ContextError](
                self.get_attribute("error_codes", use_lock=False)
            )
            if status:
                error_codes.add(error_code)
            else:
                error_codes.discard(error_code)
            self.set_attribute("error_codes", error_codes, use_lock=False)

    def clear_error_codes(self) -> None:
        self.set_attribute("error_codes", set())

    def get_file_type(self) -> ContextFileType:
        return self.get_attribute("file_type")

    def set_file_type(self, file_type_code: ContextFileType) -> None:
        self.set_attribute("file_type", file_type_code)

    def get_remote_hash(self) -> str:
        return self.get_attribute("remote_hash")

    def set_remote_hash(self, remote_hash: str) -> None:
        self.set_attribute("remote_hash", remote_hash)

    def get_parent_path(self) -> Path:
        return self.get_attribute("parent_hash")

    def set_parent_path(self, parent_hash: Path) -> None:
        self.set_attribute("parent_hash", parent_hash)

    def get_metadata(self, use_lock: bool = True) -> ContextMetadata:
        self.raise_exception_if_context_not_set()
        with self._metadata_file_lock if use_lock else nullcontext():
            self.raise_exception_if_no_metadata()
            return self._metadata[self.get_metadata_key()].model_copy()

    def set_metadata(
        self, metadata: ContextMetadata | dict, use_lock: bool = True
    ) -> None:
        self.raise_exception_if_context_not_set()
        with self._metadata_file_lock if use_lock else nullcontext():
            try:
                new_metadata = ContextMetadata.model_validate(metadata)
                self._metadata[self.get_metadata_key()] = new_metadata
            except ValidationError as e:
                setattr(e, "context", self.get_context())
                raise e

    def metadata_exists(self) -> bool:
        self.raise_exception_if_context_not_set()
        return self.get_metadata_key() in self._metadata

    def error_exists(self, use_lock: bool = True) -> bool:
        self.raise_exception_if_context_not_set()
        with self._metadata_file_lock if use_lock else nullcontext():
            self.raise_exception_if_no_metadata()
            return len(self._metadata[self.get_metadata_key()].error_codes) > 0

    def flush_metadata(self, use_lock: bool = True) -> None:
        with self._metadata_file_lock if use_lock else nullcontext():
            with open(self._metadata_file_path, "w") as metadata_file:
                metadata_file.write(
                    json.dumps(
                        self._metadata.model_dump(
                            mode="json", by_alias=self._minimise_json
                        ),
                        indent=(2 if not self._minimise_json else None),
                        separators=((",", ":") if self._minimise_json else None),
                    )
                )

    def start_flush_metadata_process(self) -> None:
        if not self._enable_flush_metadata_process:
            self._flush_metadata_proc_pool = ThreadPoolExecutor(max_workers=1)
            self._enable_flush_metadata_process = True
            self._flush_metadata_proc_future = self._flush_metadata_proc_pool.submit(
                self.flush_metadata_loop
            )

    def stop_flush_metadata_process(self) -> None:
        if self._enable_flush_metadata_process:
            self._enable_flush_metadata_process = False
            wait([self._flush_metadata_proc_future])
            self._flush_metadata_proc_pool.shutdown(wait=True, cancel_futures=False)
            self._flush_metadata_proc_pool = None
            self._flush_metadata_proc_future = None

    def flush_metadata_loop(self) -> None:
        start = time() - self._metadata_flush_seconds  # Don't wait for first cycle
        while self._enable_flush_metadata_process:
            # Loop often and check time since last flush to allow faster exiting
            if self._metadata_flush_seconds - (time() - start) <= 0:
                start = time()
                self.flush_metadata()
            sleep(5)

    def raise_exception_if_no_metadata(self) -> None:
        if not self.metadata_exists():
            raise NoMetadataError()


class InvalidMetadataFetchError(Exception):
    """Exception raised for fetching of invalid metadata key

    Attributes:
        key     -- the metadata key an invalid value submitted for
        value   -- the value submitted for the key
        message -- explanation of the error
    """

    def __init__(self, key: str, message: str | None = None) -> None:
        self.key = key

        if message is None:
            message = f"Invalid metadata key requested: {safe_str(key)}"

        super().__init__(message)


class InvalidMetadataSubmissionError(Exception):
    """Exception raised for submission of invalid metadata

    Attributes:
        key     -- the metadata key an invalid value submitted for
        value   -- the value submitted for the key
        message -- explanation of the error
    """

    def __init__(self, key: str, value: Any, message: str | None = None) -> None:
        self.key = key
        self.value = value

        if message is None:
            message = (
                f"Invalid metadata was submitted for {safe_str(key)}: {safe_str(value)}"
            )

        super().__init__(message)


class NoMetadataError(Exception):
    """Exception raised for requests for metadata that does not exist

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
            message = f"No metadata found. source_name: {safe_str(source_name)}, file_path: {safe_str(file_path)}"

        super().__init__(message)
