import numpy as np

from app.service.utils.gray_filter import gray_filter_np


class BlankDetector:
    def __init__(self, bin_threshold=150, blank_threshold_ratio=0.99999):
        self.bin_threshold = bin_threshold
        self.blank_threshold_ratio = blank_threshold_ratio

    def validate(self, image):
        is_blank = self.is_blank_image(image)
        return {"is_blank": is_blank}

    def is_blank_image(self, image: np.ndarray) -> bool:
        gray_image = gray_filter_np(image)
        binary_image = gray_image > self.bin_threshold
        white_pixel_ratio = np.mean(binary_image)
        return white_pixel_ratio >= self.blank_threshold_ratio
