import cv2
import numpy as np


def gray_filter_np(image: np.ndarray):
    return image if len(image.shape) == 2 else cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
