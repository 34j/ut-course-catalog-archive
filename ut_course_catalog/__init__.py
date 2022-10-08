"""Top-level package for ut-course-catalog."""

__author__ = """34j"""
__email__ = "34j@github.com"
__version__ = "0.1.0"
from .common import Semester, Weekday
from .ja import UTCourseCatalog, SearchParams, Details, Faculty, Institution

__all__ = [
    "Semester",
    "Weekday",
    "UTCourseCatalog",
    "SearchParams",
    "Details",
    "Faculty",
    "Institution",
]
