from .helpers import *
from .context import InitializationError, LHContext 

from pathlib import Path
from textwrap import indent

import os
import subprocess

class SevenZip(LHContext):
	_sevenzip_path = None
	_destination_root_dir = None

	def __init__(self):
		super().__init__()

	def init(self, sevenzip_path, destination_root_dir):
		if not self.get_class_init_status():
			__class__._sevenzip_path = sevenzip_path
			__class__._destination_root_dir = Path(destination_root_dir)
			self.set_class_init_status(True)
		else:
			raise InitializationError("SevenZip class has already been initialized")

	def get_destination_root_dir(self):
		return __class__._destination_root_dir
	
	def raise_exception_if_proc_failed(self, cmd_string, proc):
		if proc.returncode == 0:
			return
		else:
			raise SevenZipError(cmd_string, proc.returncode, proc.stdout, proc.stderr)
	
	def extract_archive_file(self, file_path=None, destination_dir=None):
		self.raise_exception_if_not_initialized()
		self.raise_exception_if_context_not_set()
		context = self.context
		if file_path is None:
			file_path = __class__._destination_root_dir / context["source_name"] / context["file_path"]
		if destination_dir is None:
			destination_dir = self.get_extract_root_dir()
		cmd_parts = [
			str(__class__._sevenzip_path),
			"x", "-bd", "-aoa",
			f'-o{str(destination_dir)}',
			str(file_path)
		]
		# print("RUNNING EXTRACT PROC")
		# try:
		extract_proc = subprocess.run(
			cmd_parts,
			capture_output=True,
			text=True,
			encoding='utf-8',
			universal_newlines=True,
			errors='backslashreplace'
		)
		# 	print("FINISHED EXTRACT PROC")
		# except Exception as e:
		# 	print("FAILED EXTRACT PROC")
		# 	raise e
		# print(' '.join(extract_proc.args))
		# print(extract_proc.returncode)
		# print(extract_proc.stdout)
		# print(extract_proc.stderr)
		self.raise_exception_if_proc_failed(' '.join(cmd_parts), extract_proc)
	
	def is_archive_file(self, file_path=None):
		self.raise_exception_if_not_initialized()
		self.raise_exception_if_context_not_set()
		context = self.context
		if file_path is None:
			file_path = __class__._destination_root_dir / context["source_name"] / context["file_path"]
		cmd_parts = [
			str(__class__._sevenzip_path),
			"t", "-bd", "-p",
			str(file_path)
		]
		# print(cmd_parts)
		# print("RUNNING TEST PROC")
		# try:
		extract_proc = subprocess.run(
			cmd_parts,
			capture_output=True,
			text=True,
			encoding='utf-8',
			universal_newlines=True,
			errors='backslashreplace'
		)
			# print("FINISHED TEST PROC")
		# except Exception as e:
		# 	print("FAILED TEST PROC")
		# 	raise e
		
		return extract_proc.returncode == 0
	
	def get_extract_root_dir(self, relative=False, file_path=None):
		self.raise_exception_if_not_initialized()
		self.raise_exception_if_context_not_set()
		context = self.context
		if file_path is None:
			file_path = context["file_path"]
		if relative:
			return Path(f'{file_path}.x')
		else:
			return Path(f'{__class__._destination_root_dir / context["source_name"] / file_path}.x')

class SevenZipError(Exception):
	"""Exception raised for errors that occur during 7zip operations.

	Attributes:
		rc                           -- return code of the 7zip operation
		stdout                       -- stdout of the 7zip operation
		stderr                       -- stderr of the 7zip operation
		message                      -- explanation of the error
		add_stdout_stderr_to_message -- Enables inclusion of stdout and stderr in the message
	
	Example traceback with message=None and add_stdout_stderr_to_message=True:
		Traceback (most recent call last):
			File "test.py", line 52, in <module>
				raise SevenZipError(0, "", "")
			__main__.SevenZipError: An error occured during 7zip operation. Return code was 0
				stdout:
					This is a multiline
					output found in stdout
				stderr:
					This is a multiline
					output found in stderr
						that contains indentation
	"""

	def __init__(self, cmd_string, rc, stdout, stderr, message=None, add_cmd_to_message=True, add_stdout_stderr_to_message=True):
		self.rc = rc
		self.stdout = stdout
		self.stderr = stderr
		if message is None:
			message = self.get_default_message(rc)
		if add_cmd_to_message:
			message = '\n'.join([message, f'Command: {cmd_string}'])
		if add_stdout_stderr_to_message:
			message = self.add_stdout_and_stderr_to_message(stdout, stderr, message)

		super().__init__(message)
	
	def get_default_message(self, rc):
		return f'An error occured during the 7zip operation. Return code was {rc}'
	
	def add_stdout_and_stderr_to_message(self, stdout, stderr, message):
		return os.linesep.join([line for line
			in (f'{message.strip()}\n' + indent(os.linesep.join([
			"stdout:",
			indent(f'{stdout}', "  "),
			"stderr:",
			indent(f'{stderr}', "  "),
		]), "  ")).splitlines() if line])

class SevenZipTemporaryError(SevenZipError):
	"""Exception raised for temporary errors that occur during 7zip operations.

	See SevenZipError for more information.
	"""
	def get_default_message(self, rc):
		return f'A temporary error occured during the 7zip operation. Return code was {rc}'

class SevenZipFatalError(SevenZipError):
	"""Exception raised for fatal errors that occur during 7zip operations.

	See SevenZipError for more information.
	"""
	def get_default_message(self, rc):
		return f'A fatal error occured during the 7zip operation. Return code was {rc}'
