from .helpers import *
from .context import InitializationError, LHContext 

from pathlib import Path
from textwrap import indent

import configparser
import os
import subprocess

class RClone(LHContext):
	_rclone_path = None
	_rclone_config_path = None
	_rclone_config = None
	_sources = None
	_destination_root_dir = None

	def __init__(self):
		super().__init__()

	def init(self, rclone_path, rclone_config_path, sources, destination_root_dir):
		if not self.get_class_init_status():
			__class__._rclone_path = rclone_path
			__class__._rclone_config_path = rclone_config_path
			__class__._rclone_config = configparser.ConfigParser()
			__class__._rclone_config.read(rclone_config_path)
			__class__._sources = sources
			__class__._destination_root_dir = Path(destination_root_dir)
			rclone_sources = self._rclone_config.sections()
			for source_name, source_config in sources.items():
				if source_config["remote_name"] not in rclone_sources:
					raise InitializationError(f'Configured remote name "{source_config["remote_name"]}" for source "{source_name}" not found in rclone config file.')
			self.set_source_names(sources.keys())
			self.set_class_init_status(True)
		else:
			raise InitializationError("RClone class has already been initialized")

	def get_destination_root_dir(self):
		return __class__._destination_root_dir
	
	def raise_exception_if_proc_failed(self, cmd_string, proc):
		# try:
		if proc.returncode in [0,9]:
			return
		elif proc.returncode == 5:
			raise RCloneTemporaryError(cmd_string, proc.returncode, proc.stdout, proc.stderr)
		elif proc.returncode in [1,2,3,4,6,7,8]:
			raise RCloneFatalError(cmd_string, proc.returncode, proc.stdout, proc.stderr)
		else:
			raise RCloneError(cmd_string, proc.returncode, proc.stdout, proc.stderr)
		# except Exception as e:
		# 	print("ERROR RAISING SevenZipError")
		# 	raise e

	def fetch_file_info_list(self):
		self.raise_exception_if_not_initialized()
		context = self.context
		if context["source_name"] is None:
			raise InitializationError("Source name not set in the context")
		cmd_parts = [
			str(__class__._rclone_path),
			"--config", str(__class__._rclone_config_path),
			"lsf", f'{__class__._sources[context["source_name"]]["remote_name"]}:{__class__._sources[context["source_name"]]["remote_path"]}',
			"--drive-list-chunk", "0",
			"--disable", "ListR", # Fixes incomplete results 
			"--format", "psh",
			"--separator", "|",
			"--recursive",
			"--files-only"
		]
		# print("RUNNING FETCH PROC")
		# try:
		lsf_proc = subprocess.run(
			cmd_parts,
			capture_output=True,
			text=True,
			encoding='utf-8',
			universal_newlines=True,
			errors='backslashreplace'
		)
		# 	print("FINISHED FETCH PROC")
		# except Exception as e:
		# 	print("FAILED FETCH PROC")
		# 	raise e
		
		self.raise_exception_if_proc_failed(' '.join(cmd_parts), lsf_proc)
		file_info_list = []
		for line in lsf_proc.stdout.splitlines():
			file_info = line.split("|")
			file_info_list.append((file_info[0], file_info[1], file_info[2]))
		return file_info_list
	
	def download_file(self, destination_path=None):
		self.raise_exception_if_not_initialized()
		self.raise_exception_if_context_not_set()
		context = self.context
		if destination_path is None:
			destination_path = __class__._destination_root_dir / context["source_name"] / context["file_path"]
		cmd_parts = [
			str(__class__._rclone_path),
			"--config", str(__class__._rclone_config_path),
			"copyto", f'{__class__._sources[context["source_name"]]["remote_name"]}:{str(Path(__class__._sources[context["source_name"]]["remote_path"]) / context["file_path"])}',
			str(destination_path)
		]
		# print("RUNNING DOWNLOAD PROC")
		# try:
		copyto_proc = subprocess.run(
			cmd_parts,
			capture_output=True,
			text=True,
			encoding='utf-8',
			universal_newlines=True,
			errors='backslashreplace'
		)
		# 	print("FINISHED DOWNLOAD PROC")
		# except Exception as e:
		# 	print("FAILED DOWNLOAD PROC")
		# 	raise e
		
		# print("FINISHED DOWNLOAD PROC")
		self.raise_exception_if_proc_failed(' '.join(cmd_parts), copyto_proc)

class RCloneError(Exception):
	"""Exception raised for errors that occur during rclone operations.

	Attributes:
		rc                           -- return code of the rclone operation
		stdout                       -- stdout of the rclone operation
		stderr                       -- stderr of the rclone operation
		message                      -- explanation of the error
		add_stdout_stderr_to_message -- Enables inclusion of stdout and stderr in the message
	
	Example traceback with message=None and add_stdout_stderr_to_message=True:
		Traceback (most recent call last):
			File "test.py", line 52, in <module>
				raise RCloneError(0, "", "")
			__main__.RCloneError: An error occured during rclone operation. Return code was 0
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
		return f'An error occured during the rclone operation. Return code was {rc}'
	
	def add_stdout_and_stderr_to_message(self, stdout, stderr, message):
		return os.linesep.join([line for line
			in (f'{message.strip()}\n' + indent(os.linesep.join([
			"stdout:",
			indent(f'{stdout}', "  "),
			"stderr:",
			indent(f'{stderr}', "  "),
		]), "  ")).splitlines() if line])

class RCloneTemporaryError(RCloneError):
	"""Exception raised for temporary errors that occur during rclone operations.

	See RCloneError for more information.
	"""
	def get_default_message(self, rc):
		return f'A temporary error occured during the rclone operation. Return code was {rc}'

class RCloneFatalError(RCloneError):
	"""Exception raised for fatal errors that occur during rclone operations.

	See RCloneError for more information.
	"""
	def get_default_message(self, rc):
		return f'A fatal error occured during the rclone operation. Return code was {rc}'
