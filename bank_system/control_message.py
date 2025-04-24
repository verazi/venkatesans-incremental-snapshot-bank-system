from .message import Message

class ControlMessage(Message):
    """A control message sent as part of taking a snapshot."""

    # TODO: ATTRIBUTES

    def __init__(self):
        # TODO: STUB
        pass

    def serialise(self) -> str:
        """Serialise a `ControlMessage` into a json string to be sent over a socket."""

        # TODO: STUB
        pass

    @classmethod
    def deserialise(cls, message_string: str):
        """Deserialise a json string into a `ControlMessage` object."""

        # TODO: STUB
        pass
