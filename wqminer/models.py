"""Data models for templates and simulations."""

from dataclasses import dataclass, asdict
from typing import Any, Dict, List


@dataclass
class DataField:
    field_id: str
    description: str = ""
    dataset_id: str = ""
    category_id: str = ""
    region: str = ""
    universe: str = ""
    delay: int = 1
    field_type: str = ""

    @classmethod
    def from_api(cls, payload: Dict[str, Any]) -> "DataField":
        return cls(
            field_id=payload.get("id", ""),
            description=payload.get("description", "") or "",
            dataset_id=(payload.get("dataset") or {}).get("id", "") or "",
            category_id=(payload.get("category") or {}).get("id", "") or "",
            region=payload.get("region", "") or "",
            universe=payload.get("universe", "") or "",
            delay=int(payload.get("delay", 1) or 1),
            field_type=payload.get("type", "") or "",
        )

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["id"] = payload.pop("field_id")
        payload["dataset"] = {"id": payload.pop("dataset_id")}
        payload["category"] = {"id": payload.pop("category_id")}
        payload["type"] = payload.pop("field_type")
        return payload


@dataclass
class SimulationSettings:
    region: str
    universe: str
    delay: int = 1
    neutralization: str = "INDUSTRY"
    instrument_type: str = "EQUITY"
    decay: int = 0
    truncation: float = 0.08
    pasteurization: str = "ON"
    unit_handling: str = "VERIFY"
    nan_handling: str = "OFF"
    max_trade: str = "OFF"
    language: str = "FASTEXPR"
    visualization: bool = False
    test_period: str = "P5Y0M0D"

    def to_api_payload(self, expression: str) -> Dict[str, Any]:
        return {
            "type": "REGULAR",
            "regular": expression,
            "settings": {
                "region": self.region,
                "universe": self.universe,
                "instrumentType": self.instrument_type,
                "delay": self.delay,
                "decay": self.decay,
                "neutralization": self.neutralization,
                "truncation": self.truncation,
                "pasteurization": self.pasteurization,
                "unitHandling": self.unit_handling,
                "nanHandling": self.nan_handling,
                "maxTrade": self.max_trade,
                "language": self.language,
                "visualization": self.visualization,
                "testPeriod": self.test_period,
            },
        }


@dataclass
class TemplateCandidate:
    expression: str
    source_prompt: str
    fields_used: List[str]
    operators_used: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SimulationResult:
    expression: str
    alpha_id: str
    success: bool
    sharpe: float = 0.0
    fitness: float = 0.0
    turnover: float = 0.0
    returns: float = 0.0
    drawdown: float = 0.0
    margin: float = 0.0
    passed_checks: int = 0
    total_checks: int = 0
    weight_check: str = ""
    sub_universe_sharpe: float = 0.0
    link: str = ""
    error_message: str = ""

    def score(self) -> float:
        return self.sharpe + 0.5 * self.fitness - 0.01 * self.turnover

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["score"] = self.score()
        return payload
