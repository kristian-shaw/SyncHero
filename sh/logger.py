# from .global_config import GloballyConfigured
from .helpers import *

from concurrent.futures import ThreadPoolExecutor, wait
from multiprocessing import Lock
from pathlib import Path
from time import sleep, time

# import curses

class Logger:
	_max_output_lines = None
	_progress_output_loop_seconds = None
	_log_file_path = None

	def __init__(self, log_dir, max_output_lines, progress_output_loop_seconds):
		self._outputs_lock = Lock()
		self._log_file_lock = Lock()
		self._file_counters_lock = Lock()
		self._outputs = []
		self._total_files = -1
		self._done_files = 0
		self._drawing_enabled = False
		self._draw_proc_pool = None
		self._draw_proc_future = None
		self._max_output_lines = max_output_lines
		self._progress_output_loop_seconds = progress_output_loop_seconds
		if log_dir is not None:
			log_dir = Path(log_dir)
			log_index = 0
			while (log_dir / f'log.{log_index}.txt').exists():
				log_index += 1
			self._log_file_path = log_dir / f'log.{log_index}.txt'
			self._log_file_lock.acquire()
			self._log_file_path.write_text("")
			self._log_file_lock.release()

	def start_drawing_progress(self):
		if not self._drawing_enabled:
			self._draw_proc_pool = ThreadPoolExecutor(max_workers=1)
			self._drawing_enabled = True
			self._draw_proc_future = self._draw_proc_pool.submit(self.draw_process)
	
	def stop_drawing_progress(self):
		if self._drawing_enabled:
			self._drawing_enabled = False
			wait([self._draw_proc_future])
			self._draw_proc_pool.shutdown(wait=True, cancel_futures=False)
			self._draw_proc_pool = None
			self._draw_proc_future = None
	
	def is_drawing(self):
		return self._drawing_enabled

	def submit_output(self, output_string):
		print(output_string)
		# self._outputs_lock.acquire()
		# self._outputs = self._outputs + output_string.splitlines()
		# self._outputs = self._outputs[(self._max_output_lines - 1) * -1:]
		# self._outputs_lock.release()
	
	def write_to_log_file(self, log_string):
		if self._log_file_path is not None:
			if is_str_list(log_string):
				log_string = "\n".join(log_string)
			self._log_file_lock.acquire()
			with self._log_file_path.open(mode='a', encoding='utf-8') as log_file:
				log_file.write(log_string)
			self._log_file_lock.release()
	
	def set_total_files(self, total_files):
		self._file_counters_lock.acquire()
		self._total_files = total_files
		self._file_counters_lock.release()
	
	def get_total_files(self):
		self._file_counters_lock.acquire()
		total_files = self._total_files
		self._file_counters_lock.release()
		return total_files
	
	def set_done_files(self, done_files):
		self._file_counters_lock.acquire()
		self._done_files = done_files
		self._file_counters_lock.release()
	
	def get_done_files(self):
		self._file_counters_lock.acquire()
		done_files = self._done_files
		self._file_counters_lock.release()
		return done_files

	def draw_process(self):
		# self._stdscr = curses.initscr()
		# curses.noecho()
		# curses.cbreak()
		# self._stdscr.idlok(True)
		# self._stdscr.scrollok(True)
		start = time() - self._progress_output_loop_seconds # Don't wait for first cycle
		while self._drawing_enabled:
			# Loop often and check time since last cycle to allow faster exiting
			if self._progress_output_loop_seconds - (time() - start) <= 0:
				start = time()

				# self._outputs_lock.acquire()
				# messages_to_draw = self._outputs
				# self._outputs_lock.release()
				# self._file_counters_lock.acquire()
				iteration = self._done_files
				total = self._total_files
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