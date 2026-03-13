"""Region-specific defaults aligned with common WorldQuant setups."""

from typing import Dict

DEFAULT_UNIVERSE: Dict[str, str] = {
    "USA": "TOP3000",
    "GLB": "TOP3000",
    "EUR": "TOP2500",
    "ASI": "MINVOL1M",
    "CHN": "TOP2000U",
    "IND": "TOP500",
}

DEFAULT_NEUTRALIZATION: Dict[str, str] = {
    "USA": "INDUSTRY",
    "GLB": "INDUSTRY",
    "EUR": "INDUSTRY",
    "ASI": "INDUSTRY",
    "CHN": "INDUSTRY",
    "IND": "INDUSTRY",
}


def get_default_universe(region: str) -> str:
    return DEFAULT_UNIVERSE.get(region.upper(), "TOP3000")


def get_default_neutralization(region: str) -> str:
    return DEFAULT_NEUTRALIZATION.get(region.upper(), "INDUSTRY")
