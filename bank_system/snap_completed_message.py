from dataclasses import dataclass
import json

from bank_system.action_message import ActionMessage

from .message import Message
from .process_address import ProcessAddress


@dataclass(eq=True, frozen=True)
class State:
    """The current state of the process.

    Attributes
    ----------
    money : int
        The amount of money the process has available.
    """

    money: int



@dataclass(eq=True, frozen=True)
class ProcessSnapshot:
    """A single snapshot.

    Attributes
    ----------
    state : State
        The state of the process at the time of the snapshot.
    connection_states : dict[ProcessAddress, list[ActionMessage]]
        Any messages sent to the process after the state was taken, but before the sending process
        took a snapshot.
    """

    state: State
    connection_states: dict[ProcessAddress, list[ActionMessage]]

    def serialise(self) -> str:
        connection_states = {}

        for process in self.connection_states:
            key = f"{process.address}:{process.port}"

            connection_states[key] = [json.loads(m.serialise()) for m in self.connection_states[process]]

        return json.dumps({
            "state": {
                "money": self.state.money,
            },
            "connection_states": connection_states,
        })

    @classmethod
    def deserialise(cls, message_string: str):

        raw = json.loads(message_string)

        connection_states = {}

        for process in raw["connection_states"]:
            key_parts = process.split(":")

            key = ProcessAddress(key_parts[0], int(key_parts[1]))

            connection_states[key] = { ActionMessage.deserialise(json.dumps(m)) for m in raw["connection_states"][process] }

        return cls(
            State(raw["state"]["money"]),
            connection_states
        )

class SnapCompletedMessage(Message):
    """A control message sent as part of taking a snapshot when a process has compelted a snapshot.

    Attributes
    ----------
    version : int
        The version of the snapshot that is currently being taken.
    snapshots : dict[ProcessAddress, ProcessSnapshot]
        Any snapshots of processes from further "down" the spanning tree that are being sent to
        the primary process through the tree.

    """

    MESSAGE_TYPE = "completed"

    version: int
    snapshots: dict[ProcessAddress, ProcessSnapshot]

    def __init__(self, message_from: ProcessAddress, version: int, snapshots: dict[ProcessAddress, ProcessSnapshot]):
        self.version = version
        self.snapshots = snapshots

        super().__init__(message_from)

    def serialise(self) -> str:

        snapshots = {}

        for snapshot_address in self.snapshots:
            snapshot = self.snapshots[snapshot_address]

            snapshots[f"{snapshot_address.address}:{snapshot_address.port}"] = json.loads(snapshot.serialise())

        return json.dumps({
            "type": SnapCompletedMessage.MESSAGE_TYPE,
            "message_from": {
                "address": self.message_from.address,
                "port": self.message_from.port,
            },
            "version": self.version,
            "snapshots": snapshots,
        })

    @classmethod
    def deserialise(cls, message_string: str):

        raw = json.loads(message_string)

        assert(raw["type"] == SnapCompletedMessage.MESSAGE_TYPE)

        message_from = ProcessAddress(raw["message_from"]["address"], raw["message_from"]["port"])

        snapshots = {}

        for snapshot_address_key in raw["snapshots"]:
            snapshot_address_key_parts = snapshot_address_key.split(":")
            snapshot_address = ProcessAddress(snapshot_address_key_parts[0], int(snapshot_address_key_parts[1]))

            snapshot = raw["snapshots"][snapshot_address_key]

            snapshots[snapshot_address] = ProcessSnapshot.deserialise(json.dumps(snapshot))
        return cls(message_from, raw["version"], snapshots)
