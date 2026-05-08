"""Filter range helpers for WQB-style query parameters.

Adapted from wqb (MIT License, Copyright (c) 2025 Rocky Haotian Du).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from math import inf, isinf
from typing import Any, Iterable


def _parse_ifd(value: str) -> int | float | datetime:
    text = value.strip()
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    if text and text[0] not in "-+":
        text = "+" + text
    sign = text[0]
    magnitude = text[1:].lstrip()
    if magnitude.lower() == "inf":
        parsed: int | float = inf
    elif "." in magnitude:
        parsed = float(magnitude)
    else:
        parsed = int(magnitude)
    return -parsed if sign == "-" else parsed


def _isinf(value: Any) -> bool:
    return isinstance(value, float) and isinf(value)


@dataclass(frozen=True, slots=True)
class FilterRange:
    lo: int | float | datetime = field(default=-inf)
    hi: int | float | datetime = field(default=inf)
    lo_eq: bool = field(default=False)
    hi_eq: bool = field(default=False)

    def __post_init__(self) -> None:
        if not self.lo <= self.hi:
            raise ValueError(f"not {self.lo=} <= {self.hi=}")
        if self.lo == self.hi and not (self.lo_eq and self.hi_eq):
            raise ValueError("lo == hi requires lo_eq and hi_eq")
        if _isinf(self.lo) and self.lo_eq:
            raise ValueError("lo_eq with -inf is invalid")
        if _isinf(self.hi) and self.hi_eq:
            raise ValueError("hi_eq with inf is invalid")

    @classmethod
    def from_str(cls, target: str) -> "FilterRange":
        pair = target.split(",", 1)
        if len(pair) != 2:
            raise ValueError(f"{target!r} is invalid")
        left = pair[0].strip()
        right = pair[1].strip()
        if not left or not right:
            raise ValueError(f"{target!r} is invalid")
        if left[0] == "[":
            lo_eq = True
        elif left[0] == "(":
            lo_eq = False
        else:
            raise ValueError(f"{target!r} is invalid")
        if right[-1] == "]":
            hi_eq = True
        elif right[-1] == ")":
            hi_eq = False
        else:
            raise ValueError(f"{target!r} is invalid")
        lo = _parse_ifd(left[1:])
        hi = _parse_ifd(right[:-1])
        return cls(lo, hi, lo_eq, hi_eq)

    @classmethod
    def from_conditions(cls, target: Iterable[str]) -> "FilterRange":
        lo = -inf
        hi = inf
        lo_eq = False
        hi_eq = False
        for condition in target:
            cond = condition.strip()
            if not cond:
                continue
            op = cond[:1]
            if op == ">":
                is_eq = cond[1:2] == "="
                value = _parse_ifd(cond[2 if is_eq else 1 :])
                if lo < value or (lo == value and lo_eq and not is_eq):
                    lo, lo_eq = value, is_eq
            elif op == "<":
                is_eq = cond[1:2] == "="
                value = _parse_ifd(cond[2 if is_eq else 1 :])
                if value < hi or (hi == value and hi_eq and not is_eq):
                    hi, hi_eq = value, is_eq
            elif op == "=":
                value = _parse_ifd(cond[1:])
                lo = hi = value
                lo_eq = hi_eq = True
            else:
                raise ValueError(f"{cond!r} is invalid")
        return cls(lo, hi, lo_eq, hi_eq)

    @classmethod
    def parse(cls, target: str | Iterable[str]) -> "FilterRange":
        if isinstance(target, str):
            return cls.from_str(target)
        return cls.from_conditions(target)

    def to_str(self) -> str:
        left = "[" if self.lo_eq else "("
        right = "]" if self.hi_eq else ")"
        lo = self.lo.isoformat() if isinstance(self.lo, datetime) else str(self.lo)
        hi = self.hi.isoformat() if isinstance(self.hi, datetime) else str(self.hi)
        return f"{left}{lo}, {hi}{right}"

    def to_conditions(self, *, try_eq: bool = True, inf_as: str | None = None) -> list[str]:
        if try_eq and self.lo == self.hi:
            return ["=" + str(self.lo)]
        conditions: list[str] = []
        if not (_isinf(self.lo) and inf_as is None):
            lo_val = self.lo
            if _isinf(lo_val):
                lo_val = "-" + str(inf_as)
            elif isinstance(lo_val, datetime):
                lo_val = lo_val.isoformat()
            conditions.append((">=" if self.lo_eq else ">") + str(lo_val))
        if not (_isinf(self.hi) and inf_as is None):
            hi_val = self.hi
            if _isinf(hi_val):
                hi_val = str(inf_as)
            elif isinstance(hi_val, datetime):
                hi_val = hi_val.isoformat()
            conditions.append(("<=" if self.hi_eq else "<") + str(hi_val))
        return conditions

    def to_params(self, prefix: str, **kwargs) -> str:
        return "&".join(prefix + cond for cond in self.to_conditions(**kwargs))
