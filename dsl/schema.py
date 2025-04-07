from dataclasses import dataclass
from typing import Dict, Type

@dataclass
class Schema:
    name: str
    columns: Dict[str, Type]

    def validate_column(self, column: str) -> bool:
        return column in self.columns

