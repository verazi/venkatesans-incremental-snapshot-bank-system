from enum import Enum

from .config import ProcessAddress
from .message import Message

class InitialConnectionMessage(Message):
    """A control message sent as part of taking a snapshot."""

    connecting_address: ProcessAddress

    def __init__(self, connecting_address: ProcessAddress):
        self.connecting_address = connecting_address

    def serialise(self) -> str:
        """Serialise a `ControlMessage` into a json string to be sent over a socket."""

        # TODO: STUB
        return "blah"

    @classmethod
    def deserialise(cls, message_string: str):
        """Deserialise a json string into a `ControlMessage` object."""

        # TODO: STUB
        return cls(ProcessAddress("localhost", 10101))
