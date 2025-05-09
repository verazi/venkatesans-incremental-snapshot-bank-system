from dataclasses import dataclass
from typing import Optional
from bank_system.action import Action

@dataclass
class State:
    """Represents the state of a process."""
    money: int
    last_action: Optional[Action] 