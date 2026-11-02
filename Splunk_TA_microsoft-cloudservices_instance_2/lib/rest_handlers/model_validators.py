import sys
from contextlib import contextmanager

from splunktaucclib.rest_handler.endpoint.validator import Validator, ValidationFailed


@contextmanager
def disable_exception_traceback():
    """
    All traceback information is suppressed and only the exception type and value are printed
    """
    default_value = getattr(
        sys, "tracebacklimit", 1000
    )  # `1000` is a Python's default value
    sys.tracebacklimit = 0
    try:
        yield
    finally:
        sys.tracebacklimit = default_value  # revert changes


class RestFieldValidator(Validator):
    """
    A validator that defined by user.

    The user-defined validator function should be in form:
    ``def func(value, data, *args, **kwargs): ...``
    ValidationFailed will be raised if validation failed.
    """

    def __init__(self, validate):
        """
        :param validator: user-defined validating function
        """
        super().__init__()
        self._validate = validate

    def validate(self, value, data):
        try:
            with disable_exception_traceback():
                self._validate(value, data)
        except ValidationFailed as exc:
            self.put_msg(str(exc))
            return False
        else:
            return True
