from .helpers import *
from .context import Initializable, InitializationError

from concurrent.futures import ThreadPoolExecutor, wait
from multiprocessing import Manager
from pathlib import Path
from time import sleep, time

# import curses
# import os

class Logger(Initializable):
	_outputs_lock = None
	_log_file_lock = None
	_file_counters_lock = None
	_outputs = None
	_max_output_lines = None
	_progress_output_loop_seconds = None
	_total_files = None
	_done_files = None
	_drawing_enabled = None
	_log_file_path = None
	_draw_proc_pool = None
	_draw_proc_future = None

	def __init__(self, log_dir, max_output_lines, progress_output_loop_seconds):
		super().__init__()
		if not self.get_class_init_status():
			_mp_manager = Manager()
			__class__._outputs_lock = _mp_manager.Lock()
			__class__._log_file_lock = _mp_manager.Lock()
			__class__._file_counters_lock = _mp_manager.Lock()
			__class__._outputs = []
			__class__._max_output_lines = max_output_lines
			__class__._progress_output_loop_seconds = progress_output_loop_seconds
			__class__._total_files = -1
			__class__._done_files = 0
			__class__._drawing_enabled = False
			if log_dir is not None:
				log_dir = Path(log_dir)
				log_index = 0
				while (log_dir / f'log.{log_index}.txt').exists():
					log_index += 1
				__class__._log_file_path = log_dir / f'log.{log_index}.txt'
				__class__._log_file_lock.acquire()
				__class__._log_file_path.write_text("")
				__class__._log_file_lock.release()
			self.set_class_init_status(True)
		else:
			raise InitializationError(f'Only a single {self.__class__.__name__} instance is allowed to be created.')
	
	# def __getstate__(self):
	# 	self_dict = self.__dict__.copy()
	# 	del_dict_keys(
	# 		self_dict,
	# 		[
	# 			'_outputs_lock',
	# 			'_log_file_lock',
	# 			'_file_counters_lock',
	# 			'_draw_proc_pool',
	# 			'_draw_proc_future'
	# 		]
	# 	)
	# 	return self_dict

	# def __setstate__(self, self_dict):
	# 	self.__dict__ = self_dict

	def start_drawing_progress(self):
		self.raise_exception_if_not_initialized()
		if not __class__._drawing_enabled:
			__class__._draw_proc_pool = ThreadPoolExecutor(max_workers=1)
			__class__._drawing_enabled = True
			__class__._draw_proc_future = __class__._draw_proc_pool.submit(self.draw_process)
	
	def stop_drawing_progress(self):
		self.raise_exception_if_not_initialized()
		if __class__._drawing_enabled:
			__class__._drawing_enabled = False
			wait([__class__._draw_proc_future])
			__class__._draw_proc_pool.shutdown(wait=True, cancel_futures=False)
			__class__._draw_proc_pool = None
			__class__._draw_proc_future = None
	
	def is_drawing(self):
		return self._drawing_enabled

	def submit_output(self, output_string):
		print(output_string)
		# self._outputs_lock.acquire()
		# self._outputs = self._outputs + output_string.splitlines()
		# self._outputs = self._outputs[(self._max_output_lines - 1) * -1:]
		# self._outputs_lock.release()
	
	def write_to_log_file(self, log_string):
		if __class__._log_file_path is not None:
			if is_str_list(log_string):
				log_string = "\n".join(log_string)
			__class__._log_file_lock.acquire()
			with __class__._log_file_path.open(mode='a', encoding='utf-8') as log_file:
				log_file.write(log_string)
			__class__._log_file_lock.release()
	
	def set_total_files(self, total_files):
		__class__._file_counters_lock.acquire()
		__class__._total_files = total_files
		__class__._file_counters_lock.release()
	
	def get_total_files(self):
		__class__._file_counters_lock.acquire()
		total_files = __class__._total_files
		__class__._file_counters_lock.release()
		return total_files
	
	def set_done_files(self, done_files):
		__class__._file_counters_lock.acquire()
		__class__._done_files = done_files
		__class__._file_counters_lock.release()
	
	def get_done_files(self):
		__class__._file_counters_lock.acquire()
		done_files = __class__._done_files
		__class__._file_counters_lock.release()
		return done_files

	def draw_process(self):
		# self._stdscr = curses.initscr()
		# curses.noecho()
		# curses.cbreak()
		# self._stdscr.idlok(True)
		# self._stdscr.scrollok(True)
		start = time() - __class__._progress_output_loop_seconds # Don't wait for first cycle
		while __class__._drawing_enabled:
			# Loop often and check time since last cycle to allow faster exiting
			if __class__._progress_output_loop_seconds - (time() - start) <= 0:
				start = time()

				# self._outputs_lock.acquire()
				# messages_to_draw = self._outputs
				# self._outputs_lock.release()
				# self._file_counters_lock.acquire()
				iteration = __class__._done_files
				total = __class__._total_files
				# self._file_counters_lock.release()

				# self._stdscr.clear()
				# current_idx = 0
				# for message in messages_to_draw:
				# 	self._stdscr.addstr(current_idx, 0, message)
				# 	current_idx += 1

				if total != 0:
					prefix = f"Files: {iteration}/{total}"
					suffix = "Complete"
					decimals = 1
					length = 20
					fill = "█"
					percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
					filledLength = int(length * iteration // total)
					bar = fill * filledLength + '-' * (length - filledLength)
					# self._stdscr.addstr(current_idx, 0, f"{prefix} |{bar}| {percent}% {suffix}")
					print(f"{prefix} |{bar}| {percent}% {suffix}")
					# current_idx += 1

				# self._stdscr.refresh()

				sleep(5)
		
		# curses.echo()
		# curses.nocbreak()
		# curses.endwin()

class LoggerError(Exception):
	"""Exception raised for errors that occur during initialization

	Attributes:
		message -- explanation of the error
	"""

	def __init__(self, message=None):
		if message is None:
			message = 'An error occurred in the logger'

		super().__init__(message)