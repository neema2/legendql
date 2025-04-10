from abc import ABC, abstractmethod
from typing import List

from model.schema import Schema


class DatabaseType(ABC):
    def generate_model(self, runtime: str, schemas: List[Schema]):
        return f"{self.generate_pure_runtime(runtime)}\n\n{self.generate_pure_connection()}\n\n{"\n".join(map(lambda s: self.generate_pure_database(s), schemas))}"

    @abstractmethod
    def generate_pure_runtime(self, name: str) -> str:
        pass

    @abstractmethod
    def generate_pure_connection(self) -> str:
        pass

    @abstractmethod
    def generate_pure_database(self, schema: Schema) -> str:
        pass
