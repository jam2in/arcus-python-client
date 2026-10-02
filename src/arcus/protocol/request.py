"""Messages passed from command objects to the node transport."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from ..operation import ArcusOperation


@dataclass(frozen=True, slots=True)
class CommandRequest:
    """One complete wire command and its response contract, without socket state."""

    name: str
    payload: bytes
    response: Callable[[], object] | None
    noreply: bool = False


class CommandSubmitter(Protocol):
    """The only transport capability available to command implementations."""

    def submit(self, request: CommandRequest) -> ArcusOperation: ...
