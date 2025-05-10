from dataclasses import dataclass
from typing import Dict
import json

from .process_address import ProcessAddress

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
    spanning_connections : list[ProcessAddress]
        A list of connections that form a spanning tree.
    parent: ProcessAddress | None
        The parent of the process. Following this until the parent is None will take a message to
        the primary process.
    initial_money : float
        The amount of money this process starts with.
    """

    address: ProcessAddress
    primary: bool
    connections: list[ProcessAddress]
    spanning_connections: list[ProcessAddress]
    parent: ProcessAddress | None
    initial_money: float


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

            if pconfig.parent is not None:
                parent = {"address": pconfig.parent.address, "port": pconfig.parent.port}
            else:
                parent = None

            key = f"{addr.address}:{addr.port}"
            result["nodes"][key] = {
                "address": addr.address,
                "port": addr.port,
                "primary": pconfig.primary,
                "initial_money": pconfig.initial_money,
                "connections": [
                    {"address": c.address, "port": c.port} for c in pconfig.connections
                ],
                "spanning_connections":  [
                    {"address": c.address, "port": c.port} for c in pconfig.spanning_connections
                ],
                "parent": parent,
                # "action_list": [
                #     {
                #         "to": {"address": a.to.address, "port": a.to.port},
                #         "amount": a.amount,
                #         "delay": a.delay
                #     } for a in pconfig.action_list
                # ]
            }

        return json.dumps(result)

    @classmethod
    def deserialise(cls, config_string: str):
        """
        Deserialise a json string into a Config object.
        """

        raw = json.loads(config_string)

        print(raw)

        processes: Dict[ProcessAddress, ProcessConfig] = {}

        for _, entry in raw["nodes"].items():
            addr = ProcessAddress(entry["address"], entry["port"])

            connections = [
                ProcessAddress(c["address"], c["port"]) for c in entry["connections"]
            ]

            spanning_connections = [
                ProcessAddress(c["address"], c["port"]) for c in entry["spanning_connections"]
            ]

            if entry["parent"] is not None:
                parent =  ProcessAddress(entry["parent"]["address"], entry["parent"]["port"])
            else:
                parent = None

            # actions = [
            #     Action(
            #         to=ProcessAddress(a["to"]["address"], a["to"]["port"]),
            #         amount=a["amount"],
            #         delay=a["delay"]
            #     ) for a in entry["action_list"]
            # ]

            proc_config = ProcessConfig(
                address=addr,
                primary=entry["primary"],
                connections=connections,
                spanning_connections=spanning_connections,
                parent=parent,
                initial_money=entry["initial_money"],
                # action_list=actions
            )

            processes[addr] = proc_config

        return cls(processes)



