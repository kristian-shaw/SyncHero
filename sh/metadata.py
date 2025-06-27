from .helpers import *
from .context import InitializationError, LHContext

from multiprocessing import Manager
from concurrent.futures import ThreadPoolExecutor, wait
from time import sleep, time

import json

class MetadataManager(LHContext):
	_metadata_file_path = None
	_metadata = None
	_metadata_file_lock = None
	_enable_flush_metadata_process = None
	_flush_metadata_proc_pool = None
	_flush_metadata_proc_future = None
	_metadata_flush_seconds = None

	def __init__(self):
		super().__init__()

	def init(self, source_names, metadata_file_path, metadata_flush_seconds):
		if not self.get_class_init_status():
			__class__._metadata_file_path = metadata_file_path
			with open(metadata_file_path, "r") as metadata_file:
				__class__._metadata = json.load(metadata_file)
			mp_manager = Manager()
			__class__._metadata_file_lock = mp_manager.Lock()
			__class__._enable_flush_metadata_process = False
			__class__._flush_metadata_proc_pool = None
			__class__._metadata_flush_seconds = metadata_flush_seconds
			self.set_source_names(source_names)
			for source_name in source_names:
				if source_name not in __class__._metadata:
					__class__._metadata[source_name] = {
						"files": {}
					}
			self.set_class_init_status(True)
		else:
			raise InitializationError("MetadataManager class has already been initialized.")

	# def __getstate__(self):
	# 	self_dict = self.__dict__.copy()
	# 	del_dict_keys(
	# 		self_dict,
	# 		[
	# 			'_metadata_file_lock',
	# 			'_flush_metadata_proc_pool',
	# 			'_flush_metadata_proc_future'
	# 		]
	# 	)
	# 	return self_dict

	# def __setstate__(self, self_dict):
	# 	self.__dict__ = self_dict

	def get_initialized_metadata(self, file_hash=None, is_archive=False, download_failed=False, extract_failed=False, cancelled_flag=False):
		return {
			"file_hash": file_hash,
			"is_archive": is_archive,
			"download_failed": download_failed,
			"extract_failed": extract_failed,
			"cancelled": cancelled_flag
		}
	
	def initialize_metadata_for_file(self):
		self.raise_exception_if_not_initialized()
		self.raise_exception_if_context_not_set()
		context = self.get_context()
		__class__._metadata_file_lock.acquire()
		__class__._metadata[context["source_name"]]["files"][context["file_path"]] = self.get_initialized_metadata()
		__class__._metadata_file_lock.release()

	def delete_metadata(self):
		self.raise_exception_if_not_initialized()
		self.raise_exception_if_context_not_set()
		context = self.get_context()
		__class__._metadata_file_lock.acquire()
		del __class__._metadata[context["source_name"]]["files"][context["file_path"]]
		__class__._metadata_file_lock.release()
	
	def get_attribute_for_file(self, attribute_name):
		self.raise_exception_if_not_initialized()
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
		self.raise_exception_if_not_initialized()
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
			__class__._metadata_file_lock.acquire()
			__class__._metadata[context["source_name"]]["files"][context["file_path"]][attribute_name] = attribute_value
			__class__._metadata_file_lock.release()
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
		self.raise_exception_if_not_initialized()
		self.raise_exception_if_context_not_set()
		context = self.get_context()
		__class__._metadata_file_lock.acquire()
		metadata_exists = self.metadata_exists_for_file()
		if metadata_exists:
			metadata = __class__._metadata[context["source_name"]]["files"][context["file_path"]].copy()
		__class__._metadata_file_lock.release()
		if metadata_exists:
			return metadata
		else:
			raise NoMetadataError(context["source_name"], context["file_path"])
	
	def set_metadata_for_file(self, metadata):
		self.raise_exception_if_not_initialized()
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
		self.raise_exception_if_not_initialized()
		self.raise_exception_if_context_not_set()
		context = self.get_context()
		return context["file_path"] in __class__._metadata[context["source_name"]]["files"]
	
	def error_exists_for_file(self):
		return self.get_download_failed_flag_for_file() or self.get_extract_failed_flag_for_file() or self.get_cancelled_flag_for_file()
	
	def flush_metadata_to_file(self):
		self.raise_exception_if_not_initialized()
		__class__._metadata_file_lock.acquire()
		with open(__class__._metadata_file_path, "w") as metadata_file:
			json.dump(__class__._metadata, metadata_file, indent=2)
		__class__._metadata_file_lock.release()
	
	def start_flush_metadata_process(self):
		self.raise_exception_if_not_initialized()
		if not __class__._enable_flush_metadata_process:
			__class__._flush_metadata_proc_pool = ThreadPoolExecutor(max_workers=1)
			__class__._enable_flush_metadata_process = True
			__class__._flush_metadata_proc_future = self._flush_metadata_proc_pool.submit(self.flush_metadata_loop)
	
	def stop_flush_metadata_process(self):
		self.raise_exception_if_not_initialized()
		if __class__._enable_flush_metadata_process:
			__class__._enable_flush_metadata_process = False
			wait([__class__._flush_metadata_proc_future])
			__class__._flush_metadata_proc_pool.shutdown(wait=True, cancel_futures=False)
			__class__._flush_metadata_proc_pool = None
			__class__._flush_metadata_proc_future = None
	
	def flush_metadata_loop(self):
		self.raise_exception_if_not_initialized()
		start = time() - __class__._metadata_flush_seconds # Don't wait for first cycle
		while __class__._enable_flush_metadata_process:
			# Loop often and check time since last flush to allow faster exiting
			if __class__._metadata_flush_seconds - (time() - start) <= 0:
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
