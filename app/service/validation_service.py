import numpy as np

from app.service.validation_result import ValidationResult


class ValidationService:
    def __init__(self, detectors: list):
        self.detectors = detectors

    def validate(self, image):
        result = ValidationResult()
        for detector in self.detectors:
            output = detector.validate(image)
            for k, v in output.items():
                if hasattr(result, k):
                    if isinstance(v, (np.bool_, np.bool)):
                        v = bool(v)
                    setattr(result, k, v)
        return result
