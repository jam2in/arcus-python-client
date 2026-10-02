"""Dependency-free compatibility warnings for every supported Python version."""

from functools import wraps
from typing import Callable, ParamSpec, TypeVar
import warnings


_P = ParamSpec("_P")
_R = TypeVar("_R")


def deprecated(message: str) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Mark an old entry point while preserving its signature and return value."""

    def decorate(function: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(function)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            warnings.warn(message, DeprecationWarning, stacklevel=2)
            return function(*args, **kwargs)

        wrapper.__deprecated__ = message
        return wrapper

    return decorate
