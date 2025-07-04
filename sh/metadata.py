from .helpers import *
from .context import Contextual

from multiprocessing import Lock
from concurrent.futures import ThreadPoolExecutor, wait
from time import sleep, time

import json

class MetadataManager(Contextual):
	_metadata_file_path = None
	_metadata = None
	_flush_metadata_proc_pool = None
	_flush_metadata_proc_future = None
	_metadata_flush_seconds = None

	def __init__(self, source_names, metadata_file_path, metadata_flush_seconds):
		self._metadata_file_lock = Lock()
		self._enable_flush_metadata_process = False
		self._metadata_file_path = metadata_file_path
		with open(metadata_file_path, "r") as metadata_file:
			self._metadata = json.load(metadata_file)
		self._metadata_flush_seconds = metadata_flush_seconds
		for source_name in source_names:
			if source_name not in self._metadata:
				self._metadata[source_name] = {
					"files": {}
				}
		super().__init__()

	@staticmethod
	def get_initialized_metadata(file_hash=None, is_archive=False, download_failed=False, extract_failed=False, cancelled_flag=False):
		return {
			"file_hash": file_hash,
			"is_archive": is_archive,
			"download_failed": download_failed,
			"extract_failed": extract_failed,
			"cancelled": cancelled_flag
		}
	
	def initialize_metadata_for_file(self):
		self.raise_exception_if_context_not_set()
		context = self.get_context()
		self._metadata_file_lock.acquire()
		self._metadata[context["source_name"]]["files"][context["file_path"]] = self.get_initialized_metadata()
		self._metadata_file_lock.release()

	def delete_metadata(self):
		self.raise_exception_if_context_not_set()
		context = self.get_context()
		self._metadata_file_lock.acquire()
		del self._metadata[context["source_name"]]["files"][context["file_path"]]
		self._metadata_file_lock.release()
	
	def get_attribute_for_file(self, attribute_name):
		self.raise_exception_if_context_not_set()
		file_metadata = self.get_metadata_for_file()
		try:
			return file_metadata[attribute_name]
		except KeyError:
			raise InvalidMetadataFetchError(
				attribute_name,
				f'Metadata requested for invalid key: {attribute_name}'
			)
	
	def set_attribute_for_file(self, attribute_name, attribute_type, attribute_value):
		self.raise_exception_if_context_not_set()
		if not self.metadata_exists_for_file():
			self.initialize_metadata_for_file()
		context = self.get_context()
		if attribute_name not in self.get_initialized_metadata():
			raise InvalidMetadataSubmissionError(
				attribute_name,
				attribute_value,
				f'Metadata submitted for invalid key: {attribute_name}'
			)
		if type(attribute_value) is attribute_type:
			self._metadata_file_lock.acquire()
			self._metadata[context["source_name"]]["files"][context["file_path"]][attribute_name] = attribute_value
			self._metadata_file_lock.release()
		else:
			attribute_name = format_section_str([context["source_name"], "files", context["file_path"], attribute_name])
			raise InvalidMetadataSubmissionError(
				attribute_name,
				attribute_value,
				f'Invalid metadata submitted for {attribute_name} was not of type {attribute_type.__name__}: {safe_str(attribute_value)}'
			)

	def get_hash_for_file(self):
		return self.get_attribute_for_file("file_hash")

	def set_hash_for_file(self, file_hash):
		self.set_attribute_for_file("file_hash", str, file_hash)
	
	def get_archive_flag_for_file(self):
		return self.get_attribute_for_file("is_archive")

	def set_archive_flag_for_file(self, archive_flag):
		self.set_attribute_for_file("is_archive", bool, archive_flag)

	def get_download_failed_flag_for_file(self):
		return self.get_attribute_for_file("download_failed")

	def set_download_failed_flag_for_file(self, download_failed_flag):
		self.set_attribute_for_file("download_failed", bool, download_failed_flag)

	def get_extract_failed_flag_for_file(self):
		return self.get_attribute_for_file("extract_failed")

	def set_extract_failed_flag_for_file(self, extract_failed_flag):
		self.set_attribute_for_file("extract_failed", bool, extract_failed_flag)
	
	def get_cancelled_flag_for_file(self):
		return self.get_attribute_for_file("cancelled")

	def set_cancelled_flag_for_file(self, cancelled_flag):
		self.set_attribute_for_file("cancelled", bool, cancelled_flag)
	
	def get_metadata_for_file(self):
		self.raise_exception_if_context_not_set()
		context = self.get_context()
		self._metadata_file_lock.acquire()
		metadata_exists = self.metadata_exists_for_file()
		if metadata_exists:
			metadata = self._metadata[context["source_name"]]["files"][context["file_path"]].copy()
		self._metadata_file_lock.release()
		if metadata_exists:
			return metadata
		else:
			raise NoMetadataError(context["source_name"], context["file_path"])
	
	def set_metadata_for_file(self, metadata):
		self.raise_exception_if_context_not_set()
		context = self.get_context()
		try:
			current_attribute = "file_hash"
			file_hash = metadata[current_attribute]
			current_attribute = "is_archive"
			archive_flag = metadata[current_attribute]
			current_attribute = "download_failed"
			download_failed_flag = metadata[current_attribute]
			current_attribute = "extract_failed"
			extract_failed_flag = metadata[current_attribute]
			self.set_hash_for_file(file_hash)
			self.set_archive_flag_for_file(archive_flag)
			self.set_download_failed_flag_for_file(download_failed_flag)
			self.set_extract_failed_flag_for_file(extract_failed_flag)
		except KeyError:
			raise InvalidMetadataSubmissionError(
				context["source_name"],
				context["file_path"],
				f'{current_attribute} not found in submitted metadata. source_name: {safe_str(context["source_name"])}, file_path: {safe_str(context["file_path"])}'
			)

	def metadata_exists_for_file(self):
		self.raise_exception_if_context_not_set()
		context = self.get_context()
		return context["file_path"] in self._metadata[context["source_name"]]["files"]
	
	def error_exists_for_file(self):
		return self.get_download_failed_flag_for_file() or self.get_extract_failed_flag_for_file() or self.get_cancelled_flag_for_file()
	
	def flush_metadata_to_file(self):
		self._metadata_file_lock.acquire()
		with open(self._metadata_file_path, "w") as metadata_file:
			json.dump(self._metadata, metadata_file, indent=2)
		self._metadata_file_lock.release()
	
	def start_flush_metadata_process(self):
		if not self._enable_flush_metadata_process:
			self._flush_metadata_proc_pool = ThreadPoolExecutor(max_workers=1)
			self._enable_flush_metadata_process = True
			self._flush_metadata_proc_future = self._flush_metadata_proc_pool.submit(self.flush_metadata_loop)
	
	def stop_flush_metadata_process(self):
		if self._enable_flush_metadata_process:
			self._enable_flush_metadata_process = False
			wait([self._flush_metadata_proc_future])
			self._flush_metadata_proc_pool.shutdown(wait=True, cancel_futures=False)
			self._flush_metadata_proc_pool = None
			self._flush_metadata_proc_future = None
	
	def flush_metadata_loop(self):
		start = time() - self._metadata_flush_seconds # Don't wait for first cycle
		while self._enable_flush_metadata_process:
			# Loop often and check time since last flush to allow faster exiting
			if self._metadata_flush_seconds - (time() - start) <= 0:
				start = time()
				self.flush_metadata_to_file()
			sleep(5)

class InvalidMetadataFetchError(Exception):
	"""Exception raised for fetching of invalid metadata key

	Attributes:
		key     -- the metadata key an invalid value submitted for
		value   -- the value submitted for the key
		message -- explanation of the error
	"""

	def __init__(self, key, message=None):
		self.key = key

		if message is None:
			message = f'Invalid metadata key requested: {safe_str(key)}'

		super().__init__(message)

class InvalidMetadataSubmissionError(Exception):
	"""Exception raised for submission of invalid metadata

	Attributes:
		key     -- the metadata key an invalid value submitted for
		value   -- the value submitted for the key
		message -- explanation of the error
	"""

	def __init__(self, key, value, message=None):
		self.key = key
		self.value = value

		if message is None:
			message = f'Invalid metadata was submitted for {safe_str(key)}: {safe_str(value)}'

		super().__init__(message)

class NoMetadataError(Exception):
	"""Exception raised for requests for metadata that does not exist

	Attributes:
		source_name -- the name of the source in the context
		file_path   -- the file path in the context
		message     -- explanation of the error
	"""

	def __init__(self, source_name, file_path, message=None):
		self.source_name = source_name
		self.file_path = file_path

		if message is None:
			message = f'No metadata found. source_name: {safe_str(source_name)}, file_path: {safe_str(file_path)}'

		super().__init__(message)
