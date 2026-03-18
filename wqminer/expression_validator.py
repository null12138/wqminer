"""FASTEXPR validation and preflight utilities."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .operator_store import load_operators

_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_NUMBER_RE = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)$")
_RESERVED = {
    "true",
    "false",
    "nan",
    "none",
    "null",
}


@dataclass(frozen=True)
class OperatorSpec:
    name: str
    positional_args: Tuple[str, ...]
    optional_args: Tuple[str, ...]
    allowed_values: Dict[str, Optional[Sequence[Any]]]
    from_signature: bool
    variadic: bool


@dataclass(frozen=True)
class CallSite:
    name: str
    args: Tuple[str, ...]
    start: int
    end: int
    error: str = ""


def validate_expression(
    expression: str,
    *,
    operators: Optional[Sequence[Dict[str, Any]]] = None,
    operator_file: str = "",
    region: str = "",
    delay: Optional[int] = None,
    universe: str = "",
    max_operator_count: int = 0,
    require_known_operators: bool = True,
    require_keyword_optional: bool = True,
) -> Tuple[bool, List[str], List[str]]:
    """Compatibility wrapper returning (is_valid, errors, fields_used)."""
    report = validate_expression_report(
        expression=expression,
        operators=operators,
        operator_file=operator_file,
        region=region,
        delay=delay,
        universe=universe,
        max_operator_count=max_operator_count,
        require_known_operators=require_known_operators,
        require_keyword_optional=require_keyword_optional,
    )
    return bool(report["is_valid"]), list(report["errors"]), list(report["fields_used"])


def validate_expression_report(
    expression: str,
    *,
    operators: Optional[Sequence[Dict[str, Any]]] = None,
    operator_file: str = "",
    region: str = "",
    delay: Optional[int] = None,
    universe: str = "",
    max_operator_count: int = 0,
    require_known_operators: bool = True,
    require_keyword_optional: bool = True,
) -> Dict[str, Any]:
    errors: List[str] = []
    expr = str(expression or "").strip()
    if not expr:
        errors.append("empty expression")

    if expr and not _is_parentheses_balanced(expr):
        errors.append("unbalanced parentheses")

    ops_payload = list(operators) if operators is not None else load_operators(operator_file)
    specs = _build_specs(ops_payload)
    calls = _extract_calls(expr) if expr else []
    keyword_names = set()

    for call in calls:
        if call.error:
            errors.append(f"{call.name}: {call.error}")
            continue
        spec = specs.get(call.name)
        if spec is None:
            if require_known_operators:
                errors.append(f"unknown operator: {call.name}")
            continue
        keyword_names.update(_validate_call(call, spec, errors, require_keyword_optional))

    operators_used = _unique_preserve([call.name for call in calls if call.name in specs])
    if max_operator_count and int(max_operator_count) > 0:
        estimated = estimate_operator_count(expr)
        if estimated > int(max_operator_count):
            errors.append(
                f"estimated operator count {estimated} exceeds limit {int(max_operator_count)}"
            )

    called_names = {call.name for call in calls if call.name}
    field_candidates = _extract_field_candidates(expr, set(specs.keys()).union(called_names), keyword_names)
    report = {
        "expression": expr,
        "region": region,
        "delay": delay,
        "universe": universe,
        "is_valid": len(errors) == 0,
        "errors": errors,
        "fields_used": field_candidates,
        "operators_used": operators_used,
        "operator_count_estimate": estimate_operator_count(expr),
    }
    return report


def extract_operator_names(expression: str) -> List[str]:
    calls = _extract_calls(str(expression or ""))
    return _unique_preserve([c.name for c in calls if c.name and not c.error])


def estimate_operator_count(expression: str) -> int:
    expr = str(expression or "")
    if not expr:
        return 0
    function_names = set(extract_operator_names(expr))
    arithmetic = set()
    for ch in expr:
        if ch in "+-*/":
            arithmetic.add(ch)
    return len(function_names) + len(arithmetic)


def _is_parentheses_balanced(text: str) -> bool:
    depth = 0
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch in {"'", '"'}:
            i = _skip_quoted(text, i)
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return False
        i += 1
    return depth == 0


def _skip_quoted(text: str, start: int) -> int:
    quote = text[start]
    i = start + 1
    n = len(text)
    while i < n:
        if text[i] == "\\":
            i += 2
            continue
        if text[i] == quote:
            return i + 1
        i += 1
    return n


def _find_matching_paren(text: str, open_idx: int) -> int:
    depth = 0
    i = open_idx
    n = len(text)
    while i < n:
        ch = text[i]
        if ch in {"'", '"'}:
            i = _skip_quoted(text, i)
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
            if depth < 0:
                return -1
        i += 1
    return -1


def _split_top_level(value: str, sep: str = ",") -> List[str]:
    parts: List[str] = []
    cur: List[str] = []
    depth = 0
    i = 0
    n = len(value)
    while i < n:
        ch = value[i]
        if ch in {"'", '"'}:
            end = _skip_quoted(value, i)
            cur.append(value[i:end])
            i = end
            continue
        if ch == "(":
            depth += 1
            cur.append(ch)
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            cur.append(ch)
            i += 1
            continue
        if ch == sep and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
            i += 1
            continue
        cur.append(ch)
        i += 1
    parts.append("".join(cur).strip())
    return parts


def _extract_calls(expression: str) -> List[CallSite]:
    expr = str(expression or "")
    calls: List[CallSite] = []
    i = 0
    n = len(expr)
    while i < n:
        ch = expr[i]
        if ch in {"'", '"'}:
            i = _skip_quoted(expr, i)
            continue
        if ch.isalpha() or ch == "_":
            start = i
            i += 1
            while i < n and (expr[i].isalnum() or expr[i] == "_"):
                i += 1
            name = expr[start:i]
            j = i
            while j < n and expr[j].isspace():
                j += 1
            if j < n and expr[j] == "(":
                end = _find_matching_paren(expr, j)
                if end < 0:
                    calls.append(CallSite(name=name, args=tuple(), start=start, end=n - 1, error="unclosed '('"))
                    continue
                arg_text = expr[j + 1 : end]
                args = tuple(_split_top_level(arg_text))
                calls.append(CallSite(name=name, args=args, start=start, end=end))
            continue
        i += 1
    return calls


def _build_specs(operators: Sequence[Dict[str, Any]]) -> Dict[str, OperatorSpec]:
    specs: Dict[str, OperatorSpec] = {}
    for raw in operators:
        name = str(raw.get("name", "")).strip()
        if not name:
            continue
        signature = raw.get("signature")
        if isinstance(signature, dict):
            positional = _to_name_list(signature.get("positional_args") or signature.get("required") or [])
            optional = _to_name_list(signature.get("optional") or [])
            allowed_values = signature.get("allowed_values") if isinstance(signature.get("allowed_values"), dict) else {}
            specs[name] = OperatorSpec(
                name=name,
                positional_args=tuple(positional),
                optional_args=tuple(optional),
                allowed_values=dict(allowed_values),
                from_signature=True,
                variadic=False,
            )
            continue

        positional, optional, variadic = _parse_definition_signature(
            name=name,
            definition=str(raw.get("definition", "") or ""),
            description=str(raw.get("description", "") or ""),
        )
        specs[name] = OperatorSpec(
            name=name,
            positional_args=tuple(positional),
            optional_args=tuple(optional),
            allowed_values={},
            from_signature=False,
            variadic=variadic,
        )
    return specs


def _to_name_list(value: Any) -> List[str]:
    if not isinstance(value, (list, tuple)):
        return []
    out = []
    for item in value:
        name = str(item or "").strip()
        if not name:
            continue
        out.append(name)
    return out


def _parse_definition_signature(name: str, definition: str, description: str) -> Tuple[List[str], List[str], bool]:
    if not definition:
        return [], [], False
    m = re.search(rf"\b{re.escape(name)}\s*\((.*?)\)", definition)
    if not m:
        return [], [], False
    inside = m.group(1).strip()
    if not inside:
        return [], [], False
    tokens = _split_top_level(inside)
    positional: List[str] = []
    optional: List[str] = []
    variadic = "..." in inside or "at least" in description.lower()
    for token in tokens:
        raw = token.strip()
        if not raw:
            continue
        if raw == "...":
            variadic = True
            continue
        if "=" in raw:
            key = raw.split("=", 1)[0].strip()
            m_key = _IDENTIFIER_RE.match(key)
            if m_key:
                optional.append(m_key.group(0))
            continue
        m_key = _IDENTIFIER_RE.match(raw)
        if m_key:
            positional.append(m_key.group(0))
    return positional, optional, variadic


def _split_keyword_argument(arg: str) -> Tuple[str, str]:
    depth = 0
    i = 0
    n = len(arg)
    while i < n:
        ch = arg[i]
        if ch in {"'", '"'}:
            i = _skip_quoted(arg, i)
            continue
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            i += 1
            continue
        if ch == "=" and depth == 0:
            key = arg[:i].strip()
            val = arg[i + 1 :].strip()
            if _IDENTIFIER_RE.fullmatch(key):
                return key, val
            return "", ""
        i += 1
    return "", ""


def _normalize_literal(value: str) -> str:
    raw = str(value or "").strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {"'", '"'}:
        return raw[1:-1]
    return raw


def _validate_call(
    call: CallSite,
    spec: OperatorSpec,
    errors: List[str],
    require_keyword_optional: bool,
) -> List[str]:
    positional_args: List[str] = []
    keyword_map: Dict[str, str] = {}
    keyword_names: List[str] = []
    saw_keyword = False

    for idx, token in enumerate(call.args, start=1):
        arg = str(token or "").strip()
        if not arg:
            errors.append(f"{call.name}: empty argument at position {idx}")
            continue
        key, val = _split_keyword_argument(arg)
        if key:
            if key in keyword_map:
                errors.append(f"{call.name}: duplicated keyword argument '{key}'")
            keyword_map[key] = val
            keyword_names.append(key)
            saw_keyword = True
            continue
        if saw_keyword:
            errors.append(f"{call.name}: positional argument after keyword argument at position {idx}")
        positional_args.append(arg)

    allowed = set(spec.positional_args) | set(spec.optional_args)
    if allowed:
        for key in keyword_map:
            if key not in allowed:
                errors.append(f"{call.name}: unknown parameter '{key}'")

    missing_required: List[str] = []
    for idx, pname in enumerate(spec.positional_args):
        if idx < len(positional_args):
            continue
        if pname in keyword_map:
            continue
        missing_required.append(pname)
    if missing_required:
        errors.append(f"{call.name}: missing required argument(s): {', '.join(missing_required)}")

    if spec.from_signature and require_keyword_optional and spec.optional_args:
        required = len(spec.positional_args)
        if len(positional_args) > required:
            extras = len(positional_args) - required
            errors.append(
                f"{call.name}: optional argument(s) must use name=value, found {extras} extra positional argument(s)"
            )

    if not spec.variadic:
        hard_max = len(spec.positional_args) + len(spec.optional_args)
        if hard_max > 0 and len(positional_args) > hard_max:
            errors.append(
                f"{call.name}: too many positional arguments ({len(positional_args)} > {hard_max})"
            )

    if spec.allowed_values:
        for key, allowed_values in spec.allowed_values.items():
            if allowed_values is None:
                continue
            if key not in keyword_map:
                continue
            normalized = _normalize_literal(keyword_map[key])
            allowed_norm = {_normalize_literal(str(v)) for v in allowed_values}
            if normalized not in allowed_norm:
                errors.append(
                    f"{call.name}: parameter '{key}' value '{keyword_map[key]}' not in allowed set {sorted(allowed_norm)}"
                )
    return keyword_names


def _extract_field_candidates(
    expression: str,
    operator_names: set,
    keyword_names: set,
) -> List[str]:
    out: List[str] = []
    seen = set()
    for m in _IDENTIFIER_RE.finditer(expression):
        token = m.group(0)
        low = token.lower()
        if low in _RESERVED:
            continue
        if token in operator_names:
            continue
        if token in keyword_names:
            continue
        if _NUMBER_RE.match(token):
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _unique_preserve(items: Sequence[str]) -> List[str]:
    seen = set()
    out = []
    for item in items:
        val = str(item or "").strip()
        if not val or val in seen:
            continue
        seen.add(val)
        out.append(val)
    return out
