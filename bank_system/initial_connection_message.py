from enum import Enum
import json

from .message import Message
from .process_address import ProcessAddress

class InitialConnectionMessage(Message):
    """A message sent just after connecting to another process to send the ProcessAddress of the process."""

    MESSAGE_TYPE = "initial_connection"

    def __init__(self, message_from: ProcessAddress):
        super().__init__(message_from)

    def serialise(self) -> str:
        """Serialise a `ControlMessage` into a json string to be sent over a socket."""

        return json.dumps({
            "type": InitialConnectionMessage.MESSAGE_TYPE,
            "message_from": {
                "address": self.message_from.address,
                "port": self.message_from.port,
            }
        })

    @classmethod
    def deserialise(cls, message_string: str):
        """Deserialise a json string into a `ControlMessage` object."""

        raw = json.loads(message_string)

        assert(raw["type"] == InitialConnectionMessage.MESSAGE_TYPE)

        message_from = ProcessAddress(raw["message_from"]["address"], raw["message_from"]["port"])
        return cls(message_from)
