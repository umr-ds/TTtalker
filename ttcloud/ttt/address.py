from dataclasses import dataclass


@dataclass
class TTAddress:
    address: int

    def __str__(self) -> str:
        return hex(self.address)

    def __eq__(self, other) -> bool:
        return isinstance(other, TTAddress) and (self.address == other.address)

    def __hash__(self) -> int:
        return self.address.__hash__()
