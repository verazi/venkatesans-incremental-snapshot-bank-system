from enum import Enum

from .message import Message

class ControlMessageType(Enum):
    INIT_SNAP = 0
    SNAP_COMPLETED = 1
    MARKER = 2
    ACK = 3

class ControlMessage(Message):
    """A control message sent as part of taking a snapshot."""

    message_type: ControlMessageType
    version: int

    def __init__(self, message_type: ControlMessageType, current_version: int):
        self.message_type = message_type
        self.current_version = current_version

    def serialise(self) -> str:
        """Serialise a `ControlMessage` into a json string to be sent over a socket."""

        # TODO: STUB
        pass

    @classmethod
    def deserialise(cls, message_string: str):
        """Deserialise a json string into a `ControlMessage` object."""

        # TODO: STUB
        pass
