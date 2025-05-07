from enum import Enum
import json

from .message import Message
from .process_address import ProcessAddress

class ControlMessageType(Enum):
    INIT_SNAP = 0
    SNAP_COMPLETED = 1
    MARKER = 2
    ACK = 3



class ControlMessage(Message):
    """A control message sent as part of taking a snapshot."""

    MESSAGE_TYPE = "control"

    control_message_type: ControlMessageType
    version: int

    def __init__(self, message_from: ProcessAddress, control_message_type: ControlMessageType, version: int):
        self.control_message_type = control_message_type
        self.version = version

        super().__init__(message_from)

    def serialise(self) -> str:
        """Serialise a `ControlMessage` into a json string to be sent over a socket."""

        return json.dumps({
            "type": ControlMessage.MESSAGE_TYPE,
            "message_from": {
                "address": self.message_from.address,
                "port": self.message_from.port,
            },
            "control_message_type": self.control_message_type.value,
            "version": self.version,
        })

    @classmethod
    def deserialise(cls, message_string: str):
        """Deserialise a json string into a `ControlMessage` object."""

        raw = json.loads(message_string)

        assert(raw["type"] == ControlMessage.MESSAGE_TYPE)

        message_from = ProcessAddress(raw["message_from"]["address"], raw["message_from"]["port"])
        return cls(message_from, ControlMessageType(raw["control_message_type"]), raw["version"])
