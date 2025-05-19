from dataclasses import dataclass
from typing import Optional


@dataclass
class ValidationResult:
    is_blank: Optional[bool] = None
