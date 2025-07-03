from .helpers import *
from .context import Contextual 
from .global_config import GloballyConfigured

from textwrap import indent

import os
import subprocess

class ContextualSubprocess(Contextual, GloballyConfigured):
	_executable_path = None
	_destination_root_dir = None
	
	@classmethod
	def configure(cls, executable_path, destination_root_dir):
		cls._executable_path = executable_path
		cls._destination_root_dir = destination_root_dir
		super().configure() # GloballyConfigured
	
	def get_executable_path(self):
		return self._executable_path

	def get_destination_root_dir(self):
		return self._destination_root_dir
	
	def get_destination_path(self):
		return self._destination_root_dir / self.context["source_name"] / self.context["file_path"]
	
	def run_subprocess(self, cmd_args):
		return subprocess.run(
			[self.get_executable_path()] + cmd_args,
			capture_output=True,
			text=True,
			encoding='utf-8',
			universal_newlines=True,
			errors='backslashreplace'
		)

class SubprocessError(Exception):
	"""
	Exception raised for errors that occur during subprocesses.

	Attributes:
		rc                           -- return code of the subprocess
		stdout                       -- stdout of the subprocess
		stderr                       -- stderr of the subprocess
		message                      -- explanation of the error
		add_stdout_stderr_to_message -- Enables inclusion of stdout and stderr in the message
	
	Example traceback with message=None and add_stdout_stderr_to_message=True:
		Traceback (most recent call last):
			File "test.py", line 52, in <module>
				raise SubprocessError(0, "", "")
			__main__.SubprocessError: An error occured during subprocess. Return code was 1
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
		return f'An error occured during the subprocess. Return code was {rc}'
	
	def add_stdout_and_stderr_to_message(self, stdout, stderr, message):
		return os.linesep.join([line for line
			in (f'{message.strip()}\n' + indent(os.linesep.join([
			"stdout:",
			indent(f'{stdout}', "  "),
			"stderr:",
			indent(f'{stderr}', "  "),
		]), "  ")).splitlines() if line])
