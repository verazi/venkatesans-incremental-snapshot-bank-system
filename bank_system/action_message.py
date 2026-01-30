import json

from .message import Message
from .process_address import ProcessAddress

class ActionMessage(Message):
    """A message sending money to another process.

    Attributes
    ----------
    amount : int
        The amount of money being sent.
    """

    MESSAGE_TYPE = "action"

    amount: int

    def __init__(self, message_from: ProcessAddress, amount: int):
        self.amount = amount

        super().__init__(message_from)

    def serialise(self) -> str:
        """Serialise a `ActionMessage` into a json string to be sent over a socket."""

        return json.dumps({
            "type": ActionMessage.MESSAGE_TYPE,
            "message_from": {
                "address": self.message_from.address,
                "port": self.message_from.port,
            },
            "amount": self.amount,
        })

    @classmethod
    def deserialise(cls, message_string: str):
        """Deserialise a json string into a `ActionMessage` object."""

        raw = json.loads(message_string)

        assert(raw["type"] == ActionMessage.MESSAGE_TYPE)

        message_from = ProcessAddress(raw["message_from"]["address"], raw["message_from"]["port"])
        return cls(message_from, raw["amount"])
