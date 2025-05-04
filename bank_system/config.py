from dataclasses import dataclass
from message import Message

@dataclass
class ProcessAddress:
    """The address of a process.

    Attributes
    ----------
    address : str
        The address a process will listen at.
    port : int
        The port a process will listen at.
    """

    address: str
    port: int

@dataclass
class Action():
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
    """

    address: ProcessAddress
    primary: bool
    connections: list[ProcessAddress]
    initial_money: float
    action_list: list[Action]


class Config:

    processes: dict[ProcessAddress, ProcessConfig]

    def serialise(self) -> str:
        """Serialise a config into a json string."""

        # TODO: STUB
        pass

    @classmethod
    def deserialise(cls, config_string: str):
        """Deserialise a json string into a Config object."""

        # TODO: STUB
        pass



