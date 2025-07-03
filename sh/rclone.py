from .helpers import *
from .context import InvalidContextError
from .contextual_subprocess import ContextualSubprocess, SubprocessError 
from .global_config import GlobalConfigError

from pathlib import Path

import configparser

class RClone(ContextualSubprocess):
	_rclone_config_path = None
	_rclone_config = None
	_sources = None
	
	@classmethod
	def configure(cls, rclone_path, destination_root_dir, rclone_config_path, sources):
		cls.raise_exception_if_class_configured()
		cls._rclone_config_path = rclone_config_path
		cls._rclone_config = configparser.ConfigParser()
		cls._rclone_config.read(rclone_config_path)
		cls._sources = sources
		rclone_sources = cls._rclone_config.sections()
		for source_name, source_config in sources.items():
			if source_config["remote_name"] not in rclone_sources:
				raise GlobalConfigError(f'Configured remote name "{source_config["remote_name"]}" for source "{source_name}" not found in rclone config file')
		super().configure(rclone_path, destination_root_dir)
	
	def raise_exception_if_proc_failed(self, proc):
		cmd_string = ' '.join(map(str, proc.args))
		if proc.returncode in [0,9]:
			return
		elif proc.returncode == 5:
			raise RCloneTemporaryError(cmd_string, proc.returncode, proc.stdout, proc.stderr)
		elif proc.returncode in [1,2,3,4,6,7,8]:
			raise RCloneFatalError(cmd_string, proc.returncode, proc.stdout, proc.stderr)
		else:
			raise RCloneError(cmd_string, proc.returncode, proc.stdout, proc.stderr)

	def fetch_file_info_list(self):
		self.raise_exception_if_class_not_configured()
		context = self.context
		if context["source_name"] is None:
			raise InvalidContextError(context["source_name"], context["file_path"], "Source name is not set in the context")
		cmd_args = [
			"--config", str(self._rclone_config_path),
			"lsf", f'{self._sources[context["source_name"]]["remote_name"]}:{self._sources[context["source_name"]]["remote_path"]}',
			"--drive-list-chunk", "0",
			"--disable", "ListR", # Fixes incomplete results
			"--format", "psh",
			"--separator", "|",
			"--recursive",
			"--files-only"
		]
		lsf_proc = self.run_subprocess(cmd_args)
		self.raise_exception_if_proc_failed(lsf_proc)
		file_info_list = []
		for line in lsf_proc.stdout.splitlines():
			file_info = line.split("|")
			file_info_list.append((file_info[0], file_info[1], file_info[2]))
		return file_info_list
	
	def download_file(self):
		self.raise_exception_if_class_not_configured()
		self.raise_exception_if_context_not_set()
		context = self.context
		cmd_args = [
			"--config", str(self._rclone_config_path),
			"copyto", f'{self._sources[context["source_name"]]["remote_name"]}:{str(Path(self._sources[context["source_name"]]["remote_path"]) / context["file_path"])}',
			str(self.get_destination_path())
		]
		copyto_proc = self.run_subprocess(cmd_args)
		self.raise_exception_if_proc_failed(copyto_proc)

class RCloneError(SubprocessError):
	"""
	Exception raised for errors that occur during rclone operations.
	"""
	def get_default_message(self, rc):
		return f'An error occured during the rclone operation. Return code was {rc}'

class RCloneTemporaryError(RCloneError):
	"""
	Exception raised for temporary errors that occur during rclone operations.

	See RCloneError for more information.
	"""
	def get_default_message(self, rc):
		return f'A temporary error occured during the rclone operation. Return code was {rc}'

class RCloneFatalError(RCloneError):
	"""
	Exception raised for fatal errors that occur during rclone operations.

	See RCloneError for more information.
	"""
	def get_default_message(self, rc):
		return f'A fatal error occured during the rclone operation. Return code was {rc}'
