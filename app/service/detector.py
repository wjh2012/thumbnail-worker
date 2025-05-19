from abc import ABC, abstractmethod


class Detector(ABC):

    @abstractmethod
    def validate(self, image):
        pass
