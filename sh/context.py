from .helpers import *

from multiprocessing import Manager

class InitTracker:
	def __new__(cls):
		if cls._self is None:
			cls._self = super().__new__(cls)
		return cls._self

	def __init__(self):
		if __class__._class_init_statuses is None:
			__class__._class_init_statuses = dict()
	
	@classmethod
	def get_class_init_status(cls, class_name):
		if cls._class_init_statuses is None:
			raise InitializationError(f'{cls.__name__} class has not been initialized')
		elif class_name in cls._class_init_statuses:
			return cls._class_init_statuses[class_name]
		else:
			return False
	
	@classmethod
	def set_class_init_status(cls, class_name, status):
		if cls._class_init_statuses is None:
			raise InitializationError(f'{cls.__name__} class has not been initialized')
		cls._class_init_statuses[class_name] = status

class Initializable:
	def __init__(self):
		if __class__._init_tracker is None:
			__class__._init_tracker = InitTracker()
			
	def get_class_init_status(self):
		if __class__._init_tracker is None:
			return False
		else:
			return __class__._init_tracker.get_class_init_status(self.__class__.__name__)
	
	def set_class_init_status(self, status):
		__class__._init_tracker.set_class_init_status(self.__class__.__name__, status)
	
	def raise_exception_if_not_initialized(self):
		if not self.get_class_init_status():
			raise InitializationError(
				f'An instance of {self.__class__.__name__} has not yet been created.'
			)

class LHContext(Initializable):
	def __init__(self):
		super().__init__()
		if not self.get_class_init_status():
			mp_manager = Manager()
			__class__._context_lock = mp_manager.Lock()
			__class__._context_hashes = dict()
		if self.__class__.__name__ not in __class__._context_hashes:
			__class__._context_hashes[self.__class__.__name__] = set()
		self.context = {
			"source_name": None,
			"file_path": None
		}
	
	def set_source_names(self, source_names):
		__class__._source_names = source_names

	def get_context_source_name(self):
		self.raise_exception_if_not_initialized()
		return self.context["source_name"]

	def set_context_source_name(self, source_name):
		self.raise_exception_if_not_initialized()
		if type(source_name) != str:
			raise InvalidContextSubmissionError(
				"source_name",
				source_name,
				f'Invalid value submitted for context source_name was not of type str: {safe_str(source_name)}'
			)
		elif source_name not in __class__._source_names:
			raise InvalidContextSubmissionError(
				"source_name",
				source_name,
				f'Invalid value submitted for context source_name was not a configured source: {safe_str(source_name)}'
			)
		else:
			self.context["file_path"] = None
			self.context["source_name"] = source_name

	def get_context_file_path(self):
		self.raise_exception_if_not_initialized()
		return self.context["file_path"]

	def set_context_file_path(self, file_path):
		self.raise_exception_if_not_initialized()
		context = self.get_context()
		if context["source_name"] == None:
			raise InvalidContextError(
				context["source_name"],
				context["file_path"],
				f'Context does not have source_name set'
			)
		elif not isinstance(file_path, str):
			raise InvalidContextSubmissionError(
				"file_path",
				context["file_path"],
				f'Invalid value submitted for context file_path was not of type str: {safe_str(file_path)}'
			)
		else:
			__class__._context_lock.acquire()
			if self.context_is_set():
				__class__._context_hashes[self.__class__.__name__].remove(self.get_context_hash())
			self.context["file_path"] = file_path
			context_hash = self.get_context_hash()
			if context_hash in __class__._context_hashes[self.__class__.__name__]:
				self.context["file_path"] = None
				raise InvalidContextSubmissionError(
					context["source_name"],
					file_path,
					f'Context hash is already in use. source_name: {context["source_name"]}, file_path: {safe_str(file_path)}'
				)
			else:
				__class__._context_hashes[self.__class__.__name__].add(self.get_context_hash())
			__class__._context_lock.release()
	
	def get_context(self):
		self.raise_exception_if_not_initialized()
		return self.context.copy()

	def set_context(self, context):
		self.raise_exception_if_not_initialized()
		context = context.copy()
		if type(context) != dict:
			raise InvalidContextSubmissionError(
				"context",
				context,
				f'Invalid value submitted for context was not of type dict: {safe_str(context)}'
			)
		elif "source_name" not in context:
			raise InvalidContextSubmissionError(
				"context",
				context,
				f'Invalid value submitted for context was missing source_name key'
			)
		elif "file_path" not in context:
			raise InvalidContextSubmissionError(
				"context",
				context,
				f'Invalid value submitted for context was missing file_path key'
			)
		else:
			self.set_context_source_name(context["source_name"])
			self.set_context_file_path(context["file_path"])
	
	def get_context_hash(self):
		self.raise_exception_if_not_initialized()
		self.raise_exception_if_context_not_set()
		context = self.context
		return get_context_hash(context["source_name"], context["file_path"])
	
	def context_is_set(self):
		self.raise_exception_if_not_initialized()
		context = self.context
		return context["file_path"] is not None

	def free_context(self):
		self.raise_exception_if_not_initialized()
		if self.context_is_set():
			__class__._context_hashes[self.__class__.__name__].remove(self.get_context_hash())
		self.context["file_path"] = None
		self.context["source_name"] = None
	
	def raise_exception_if_not_initialized(self):
		super().raise_exception_if_not_initialized()
		if __class__._source_names is None:
			raise InitializationError(
				f'Source names have not been configured'
			)
	
	def raise_exception_if_context_not_set(self):
		context = self.context
		if not self.context_is_set():
			raise InvalidContextError(
				context["source_name"],
				context["file_path"],
				f'Context is not set for any file. source_name: {context["source_name"]}, file_path: {safe_str(context["file_path"])}'
			)

class InitializationError(Exception):
	"""Exception raised for errors that occur during initialization

	Attributes:
		message -- explanation of the error
	"""

	def __init__(self, message=None):
		if message is None:
			message = 'An initialization related error occurred'

		super().__init__(message)

class InvalidContextSubmissionError(Exception):
	"""Exception raised for submission of invalid metadata

	Attributes:
		atttibute_name -- the name of attribute in the context
		value          -- the value submitted for the attribute
		message        -- explanation of the error
	"""

	def __init__(self, atttibute_name, value, message=None):
		self.atttibute_name = atttibute_name
		self.value = value

		if message is None:
			message = f'Invalid value submitted for context attribute {safe_str(atttibute_name)}: {safe_str(value)}'

		super().__init__(message)

class InvalidContextError(Exception):
	"""Exception raised for invalid context

	Attributes:
		source_name -- the name of the source in the context
		file_path   -- the file path in the context
		message     -- explanation of the error
	"""

	def __init__(self, source_name, file_path, message=None):
		self.source_name = source_name
		self.file_path = file_path

		if message is None:
			message = f'Invalid context. source_name: {safe_str(source_name)}, file_path: {safe_str(file_path)}'

		super().__init__(message)
