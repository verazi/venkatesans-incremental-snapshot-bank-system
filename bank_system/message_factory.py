from .message import Message

class MessageFactory:
    """A factory for de-serialising messages."""

    @staticmethod
    def deserialise(data: str) -> Message:
        pass