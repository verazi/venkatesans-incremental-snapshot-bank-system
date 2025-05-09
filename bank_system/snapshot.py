from dataclasses import dataclass
from typing import Set
from bank_system.state import State

@dataclass
class Snapshot:
    """Represents a snapshot of a process."""
    state: State
    connection_states: Set[State] 