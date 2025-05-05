from dataclasses import dataclass


@dataclass(frozen=True, eq=True)
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

    def __repr__(self):
        return f"{self.address}:{self.port}"