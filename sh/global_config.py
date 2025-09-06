_class_init_statuses: dict[bool] = dict[bool]()


class GloballyConfigured:
    """ """

    @classmethod
    def is_class_configured(cls) -> bool:
        if cls.__name__ in _class_init_statuses:
            return _class_init_statuses[cls.__name__]
        else:
            return False

    @classmethod
    def configure(cls) -> None:
        _class_init_statuses[cls.__name__] = True

    @classmethod
    def raise_exception_if_class_not_configured(cls) -> None:
        if not cls.is_class_configured():
            raise GlobalConfigError(f"Class {cls.__name__} has not yet been configured")

    @classmethod
    def raise_exception_if_class_configured(cls) -> None:
        if cls.is_class_configured():
            raise GlobalConfigError(f"Class {cls.__name__} has already been configured")


class GlobalConfigError(Exception):
    """
    Exception raised for issues with the global config of a class

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message: str | None = None) -> None:
        if message is None:
            message = "A class global config related error occurred"

        super().__init__(message)
