from dataclasses import dataclass
from bank_system.process_address import ProcessAddress
from bank_system.action_message import ActionMessage

@dataclass
class Action:
    """Represents a money transfer action."""
    to: ProcessAddress
    amount: int
    delay: float
    
    def to_message(self, from_address: ProcessAddress) -> ActionMessage:
        """Convert this action to a message."""
        return ActionMessage(
            message_from=from_address,
            amount=self.amount
        ) 