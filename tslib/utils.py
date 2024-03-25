import warnings


def warn(message):
    warnings.warn(message, stacklevel=2)
