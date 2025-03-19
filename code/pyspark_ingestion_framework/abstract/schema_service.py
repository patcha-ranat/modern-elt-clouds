from abc import ABC, abstractmethod

class AbstractSchemaService(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def generate(self):
        pass

    @abstractmethod
    def log(self):
        pass

    @abstractmethod
    def process(self):
        pass
