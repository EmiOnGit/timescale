import warnings


def warn(message):
    warnings.warn(str(message), stacklevel=2)
