from abc import ABC, abstractmethod


class AbstractArgumentService(ABC):

    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def log(self):
        pass

    @abstractmethod
    def process(self):
        pass
