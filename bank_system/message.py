from abc import ABC, abstractmethod


class Message(ABC):
    """An abstract method that can be sent to and from a process."""

    @abstractmethod
    def serialise(self) -> str:
        """Serialise a message into a json string to be sent over a socket."""
        pass

    @abstractmethod
    @classmethod
    def deserialise(cls, message_string: str):
        """Deserialise a json string into a Message object."""
        pass
