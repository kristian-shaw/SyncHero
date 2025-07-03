from .helpers import *

from dataclasses import dataclass
from multiprocessing import Manager, Lock

class Contextual:
	_context_lock = Lock()
	_active_context_hashes = set()
	
	def __init__(self):
		self.context = {
			"source_name": None,
			"file_path": None
		}
	
	@staticmethod
	def lock_context_changes():
		__class__._context_lock.acquire()
	
	@staticmethod
	def unlock_context_changes():
		__class__._context_lock.release()

	def get_context_source_name(self):
		return self.context["source_name"]

	def set_context_source_name(self, source_name):
		if not isinstance(source_name, str):
			raise InvalidContextSubmissionError(
				"source_name",
				source_name,
				f'Invalid value submitted for context source_name was not of type str: {safe_str(source_name)}'
			)
		else:
			self.lock_context_changes()
			self.context["file_path"] = None
			self.context["source_name"] = source_name
			self.unlock_context_changes()

	def get_context_file_path(self):
		return self.context["file_path"]

	def set_context_file_path(self, file_path):
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
			self.lock_context_changes()
			if self.context_is_set():
				__class__._active_context_hashes.remove(self.get_context_hash())
			self.context["file_path"] = file_path
			context_hash = self.get_context_hash()
			if context_hash in __class__._active_context_hashes:
				self.context["file_path"] = None
				self.unlock_context_changes()
				raise InvalidContextSubmissionError(
					context["source_name"],
					file_path,
					f'Context is already in use. source_name: {context["source_name"]}, file_path: {safe_str(file_path)}'
				)
			else:
				__class__._active_context_hashes.add(self.get_context_hash())
				self.unlock_context_changes()
	
	def get_context(self):
		return self.context.copy()

	def set_context(self, context):
		context = context.copy()
		if not isinstance(context, dict):
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
		self.raise_exception_if_context_not_set()
		context = self.context
		return get_context_hash(context["source_name"], context["file_path"])
	
	def context_is_set(self):
		context = self.context
		return context["file_path"] is not None

	def free_context(self):
		self.lock_context_changes()
		if self.context_is_set():
			__class__._active_context_hashes.remove(self.get_context_hash())
		self.context["file_path"] = None
		self.context["source_name"] = None
		self.unlock_context_changes()
	
	def raise_exception_if_context_not_set(self):
		context = self.context
		if not self.context_is_set():
			raise InvalidContextError(
				context["source_name"],
				context["file_path"],
				f'Context is not set. source_name: {context["source_name"]}, file_path: {safe_str(context["file_path"])}'
			)

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
