from dataclasses import dataclass
from typing import Dict
import json

from .action_message import ActionMessage
from .process_address import ProcessAddress

@dataclass(eq=True, frozen=True)
class Action:
    """An action to take in the system

    Attributes
    ----------
    to : ProcessAddress
        The process to send money to.
    amount : float
        The amount of money to send.
    delay : int
        The time to wait after doing the action before doing the next action.
    """

    to: ProcessAddress
    amount: float
    delay: int

    def serialise(self) -> str:
        return json.dumps({
            "type": "action",
            "to": {"address": self.to.address, "port": self.to.port},
            "amount": self.amount
        })

    @classmethod
    def deserialise(cls, message_string: str):
        obj = json.loads(message_string)
        addr = ProcessAddress(obj["to"]["address"], obj["to"]["port"])
        return cls(to=addr, amount=obj["amount"], delay=0)

    def to_message(self, message_from: ProcessAddress) -> ActionMessage:
        return ActionMessage(message_from, self.amount)

@dataclass
class ProcessConfig:
    """A single processes config.

    Attributes
    ----------
    address : ProcessAddress
        The address this process will listen on. Also used to uniquely identify the process.
    primary : bool
        If this process is the primary process in charge of starting snapshots.
    connections : list[ProcessAddress]
        A list of all direct connections to this process. If a process appears in the list this
        process must also appear in the connections list for that process.
    initial_money : float
        The amount of money this process starts with.
    action_list : list[Action]
        A list of actions to take in the system.
    parent : ProcessAddress | None
        The parent process in the snapshot tree (None for primary).
    children : list[ProcessAddress]
        The child processes in the snapshot tree.
    """

    address: ProcessAddress
    primary: bool
    connections: list[ProcessAddress]
    initial_money: float
    action_list: list[Action]
    parent: ProcessAddress | None
    children: list[ProcessAddress]


class Config:

    processes: dict[ProcessAddress, ProcessConfig]

    def __init__(self, processes: dict[ProcessAddress, ProcessConfig]):
        self.processes = processes

    def serialise(self) -> str:
        """
        Serialise a config into a json string.
        """

        result = {"nodes": {}}

        for addr, pconfig in self.processes.items():
            key = f"{addr.address}:{addr.port}"
            result["nodes"][key] = {
                "address": addr.address,
                "port": addr.port,
                "primary": pconfig.primary,
                "initial_money": pconfig.initial_money,
                "connections": [
                    {"address": c.address, "port": c.port} for c in pconfig.connections
                ],
                "action_list": [
                    {
                        "to": {"address": a.to.address, "port": a.to.port},
                        "amount": a.amount,
                        "delay": a.delay
                    } for a in pconfig.action_list
                ]
            }

        return json.dumps(result)

    @classmethod
    def deserialise(cls, config_string: str):
        """
        Deserialise a json string into a Config object.
        """

        raw = json.loads(config_string)
        processes: Dict[ProcessAddress, ProcessConfig] = {}

        for _, entry in raw["nodes"].items():
            addr = ProcessAddress(entry["address"], entry["port"])

            connections = [
                ProcessAddress(c["address"], c["port"]) for c in entry["connections"]
            ]

            actions = [
                Action(
                    to=ProcessAddress(a["to"]["address"], a["to"]["port"]),
                    amount=a["amount"],
                    delay=a["delay"]
                ) for a in entry["action_list"]
            ]

            proc_config = ProcessConfig(
                address=addr,
                primary=entry["primary"],
                connections=connections,
                initial_money=entry["initial_money"],
                action_list=actions,
                parent=None,
                children=[]
            )

            processes[addr] = proc_config

        return cls(processes)



