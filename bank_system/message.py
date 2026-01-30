from __future__ import annotations
from typing import TYPE_CHECKING

from abc import ABC, abstractmethod

from .process_address import ProcessAddress


class Message(ABC):
    """An abstract method that can be sent to and from a process.

    Attributes
    ----------
    message_from : ProcessAddress
        The process that sent the message.
    """

    message_from: ProcessAddress

    @abstractmethod
    def __init__(self, message_from: ProcessAddress):
        self.message_from = message_from


    @abstractmethod
    def serialise(self) -> str:
        """Serialise a message into a json string to be sent over a socket."""
        pass

    @classmethod
    @abstractmethod
    def deserialise(cls, message_string: str):
        """Deserialise a json string into a Message object."""
        pass
