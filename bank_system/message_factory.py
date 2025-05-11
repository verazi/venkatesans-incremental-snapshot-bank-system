import json

from .snap_completed_message import SnapCompletedMessage
from .action_message import ActionMessage
from .initial_connection_message import InitialConnectionMessage
from .control_message import ControlMessage
from .message import Message

class MessageFactory:
    """A factory for de-serialising messages."""

    @staticmethod
    def deserialise(data: str) -> Message:

        raw = json.loads(data)

        match raw["type"]:
            case ControlMessage.MESSAGE_TYPE:
                return ControlMessage.deserialise(data)
            case InitialConnectionMessage.MESSAGE_TYPE:
                return InitialConnectionMessage.deserialise(data)
            case ActionMessage.MESSAGE_TYPE:
                return ActionMessage.deserialise(data)
            case SnapCompletedMessage.MESSAGE_TYPE:
                return SnapCompletedMessage.deserialise(data)