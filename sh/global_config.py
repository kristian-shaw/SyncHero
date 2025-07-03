class GloballyConfigured:
	"""
	
	"""
	_class_init_statuses = dict()

	@classmethod
	def is_class_configured(cls):
		if cls.__name__ in __class__._class_init_statuses: 
			return __class__._class_init_statuses[cls.__name__] 
		else:
			return False
	
	@classmethod
	def configure(cls):
		__class__._class_init_statuses[cls.__name__] = True
	
	@classmethod
	def raise_exception_if_class_not_configured(cls):
		if not cls.is_class_configured():
			raise GlobalConfigError(
				f'Class {cls.__name__} has not yet been configured'
			)
	
	@classmethod
	def raise_exception_if_class_configured(cls):
		if cls.is_class_configured():
			raise GlobalConfigError(
				f'Class {cls.__name__} has already been configured'
			)

class GlobalConfigError(Exception):
	"""
	Exception raised for issues with the global config of a class

	Attributes:
		message -- explanation of the error
	"""

	def __init__(self, message=None):
		if message is None:
			message = 'A class global config related error occurred'

		super().__init__(message)