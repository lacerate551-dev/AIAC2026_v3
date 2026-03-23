# -*- coding: utf-8 -*-
"""
Expression Type Checker (v2)

Purpose:
- Perform static type checking before backtesting to catch obvious type mismatches
- Example: ts_delta(event_field, 5) / scale(event_field) -> BRAIN will report "does not support event inputs"

Design choices (v2):
- Lightweight function call scanning + recursive argument parsing (no full AST)
- Only enforce types for operators defined in operators_metadata.json
- Operators with "any" in inputs are skipped (loose mode)
- Constants (numbers, strings) are recognized as scalar type
- Nested calls supported: rank(ts_mean(field, 20)) recursively checks innermost field type
- Type compatibility rules: MATRIX normalized to vector, usable for numeric operations

BRAIN Platform Type Semantics (verified 2026-03-19):
- vector (MATRIX type): Can be used directly with ts_mean/rank/zscore/scale etc.
- event (VECTOR type): Event-type data, requires vec_sum/vec_avg/vec_count operators to convert to vector
- group: Grouping fields, only for second argument of group_neutralize etc.
- symbol: Identifier fields (ticker, cusip), cannot be used for numeric calculations

Public interfaces:
- load_operator_metadata()
- check_expression_types(expression, field_type_index, expression_id=None)
- is_type_compatible(actual_type, expected_types)
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


CONFIG_OPERATORS_METADATA = Path(__file__).resolve().parent.parent / "config" / "operators_metadata.json"


Scalar = Union[int, float, str]


# ==================== Type Compatibility Rules ====================

# Type compatibility mapping: actual_type -> set of compatible expected types
# Updated 2026-03-19: VECTOR type is event-type data, requires vec_* conversion
TYPE_COMPATIBILITY = {
    # vector type (MATRIX fields) can be used wherever vector is expected
    "vector": {"vector", "any"},
    # group type only for group-expected positions
    "group": {"group", "any"},
    # symbol type cannot be used for numeric calculations
    "symbol": {"symbol", "any"},
    # event type (VECTOR fields) requires vec_* operators (vec_sum/vec_avg/vec_count)
    "event": {"event", "any"},
    # scalar can be used for numeric parameter positions
    "scalar": {"scalar", "int", "any"},
    # int is a subset of scalar
    "int": {"int", "scalar", "any"},
    # unknown type is handled loosely
    "unknown": {"any"},
    # matrix is an alias for vector (BRAIN platform semantics)
    "matrix": {"vector", "any"},
}


def is_type_compatible(actual_type: str, expected_types: List[str]) -> bool:
    """
    Check if actual type is compatible with expected types.

    Args:
        actual_type: The field's actual type (normalized_type)
        expected_types: List of types the operator expects

    Returns:
        True if compatible, False otherwise
    """
    actual = actual_type.lower().strip()
    if not expected_types:
        return True

    # Get the set of compatible expected types for this actual type
    compatible_set = TYPE_COMPATIBILITY.get(actual, set())

    # Check for intersection
    for expected in expected_types:
        expected = expected.lower().strip()
        if expected in compatible_set:
            return True
        # Direct match
        if actual == expected:
            return True
        # any type is compatible with everything
        if expected == "any" or actual == "any":
            return True

    return False


@dataclass(frozen=True)
class OperatorSignature:
    name: str
    inputs: List[List[str]]  # Allowed types for each parameter (ordered by position)
    output: str
    basis: str = ""


def load_operator_metadata(path: Optional[Path] = None) -> Dict[str, OperatorSignature]:
    """Load operators_metadata.json, return name -> signature mapping."""
    p = Path(path) if path is not None else CONFIG_OPERATORS_METADATA
    if not p.exists():
        logger.warning("operators_metadata.json not found: %s (will run with empty signature table)", p)
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.warning("Failed to read operators_metadata.json: %s (will run with empty signature table)", e)
        return {}

    out: Dict[str, OperatorSignature] = {}
    if not isinstance(data, list):
        return out
    for item in data:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        if not name:
            continue
        inputs_raw = item.get("inputs") or []
        # inputs: [{position, type:[...]}] -> List[List[str]] ordered by position
        inputs_sorted = sorted(
            [x for x in inputs_raw if isinstance(x, dict) and "position" in x],
            key=lambda x: int(x.get("position", 1)),
        )
        inputs: List[List[str]] = []
        for x in inputs_sorted:
            t = x.get("type")
            if isinstance(t, list) and t:
                inputs.append([str(v).lower() for v in t])
            else:
                inputs.append(["any"])
        out[name.lower()] = OperatorSignature(
            name=name.lower(),
            inputs=inputs,
            output=str(item.get("output", "unknown")).lower(),
            basis=str(item.get("basis", "")),
        )
    return out


def build_field_type_index(field_metadata: List[Dict[str, Any]]) -> Dict[str, str]:
    """Build field_id/field_name -> normalized_type index from field_metadata."""
    index: Dict[str, str] = {}
    for row in field_metadata or []:
        if not isinstance(row, dict):
            continue
        fid = str(row.get("field_id") or "").strip()
        fname = str(row.get("field_name") or "").strip()
        nt = str(row.get("normalized_type") or row.get("type") or "unknown").strip().lower()
        if fid:
            index[fid] = nt
        if fname:
            index[fname] = nt
    return index


# -------------------- Lightweight Parser --------------------

_RE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RE_NUMBER = re.compile(r"^-?\d+(\.\d+)?$")


def _split_args(arg_str: str) -> List[str]:
    """Split arguments by comma, handling nested parentheses/strings."""
    args: List[str] = []
    buf: List[str] = []
    depth = 0
    in_str: Optional[str] = None
    escape = False
    for ch in arg_str:
        if in_str:
            buf.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == in_str:
                in_str = None
            continue
        if ch in ("'", '"'):
            in_str = ch
            buf.append(ch)
            continue
        if ch == "(":
            depth += 1
            buf.append(ch)
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            buf.append(ch)
            continue
        if ch == "," and depth == 0:
            s = "".join(buf).strip()
            if s:
                args.append(s)
            buf = []
            continue
        buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        args.append(tail)
    return args


def _parse_expr_type(
    expr: str,
    field_type_index: Dict[str, str],
    op_sigs: Dict[str, OperatorSignature],
    expression_id: Optional[str] = None,
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Infer expression type and return list of errors.
    Returns (type, errors).
    """
    s = (expr or "").strip()
    if not s:
        return "unknown", []

    # String constant
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return "scalar", []
    # Number constant
    if _RE_NUMBER.match(s):
        return "scalar", []

    # Pattern: func(...)
    m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)$", s)
    if m:
        func = m.group(1).strip().lower()
        inner = m.group(2).strip()
        args = _split_args(inner)
        arg_types: List[str] = []
        errors: List[Dict[str, Any]] = []
        for a in args:
            t, es = _parse_expr_type(a, field_type_index, op_sigs, expression_id=expression_id)
            arg_types.append(t)
            errors.extend(es)

        sig = op_sigs.get(func)
        if not sig:
            # Unknown operator: handle loosely, no error
            return "unknown", errors

        # If any parameter expects "any", skip this operator's check (loose mode)
        if any(("any" in (allowed or [])) for allowed in sig.inputs):
            return sig.output or "unknown", errors

        # Validate argument count (conservative: check if signature exists)
        if sig.inputs and len(arg_types) < len(sig.inputs):
            errors.append({
                "error_type": "arity_mismatch",
                "operator": func,
                "expected_arity": len(sig.inputs),
                "actual_arity": len(arg_types),
                "subexpr": s[:160],
                "expression_id": expression_id,
            })
            return sig.output or "unknown", errors

        # Validate argument types by position (using compatibility rules)
        for idx, allowed in enumerate(sig.inputs):
            if idx >= len(arg_types):
                break
            actual = arg_types[idx]
            # Use type compatibility function for checking
            if allowed and actual != "unknown":
                if not is_type_compatible(actual, allowed):
                    errors.append({
                        "error_type": "type_mismatch",
                        "operator": func,
                        "arg_index": idx + 1,
                        "expected": allowed,
                        "actual": actual,
                        "subexpr": s[:160],
                        "expression_id": expression_id,
                    })

        return sig.output or "unknown", errors

    # Variable/field name
    if _RE_IDENT.match(s):
        return field_type_index.get(s, "unknown"), []

    # Other complex expressions (a-b, a*b, -a, etc.): v1 doesn't parse operators, return unknown loosely
    return "unknown", []


def check_expression_types(
    expression: str,
    field_type_index: Dict[str, str],
    operator_metadata: Optional[Dict[str, OperatorSignature]] = None,
    expression_id: Optional[str] = None,
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Check if expression types are valid.

    Args:
        expression: Alpha regular expression
        field_type_index: field_id/field_name -> normalized_type
        operator_metadata: Optional, if not provided will auto-load from config/operators_metadata.json
        expression_id: Optional, for logging/error identification

    Returns:
        (ok, error)
        - ok=True means no type mismatch errors found
        - error is structured error info (first critical error), for logging/self-healing
    """
    op_sigs = operator_metadata or load_operator_metadata()
    _, errors = _parse_expr_type(expression, field_type_index, op_sigs, expression_id=expression_id)
    # Only treat type_mismatch / arity_mismatch as hard errors
    hard = [e for e in errors if e.get("error_type") in ("type_mismatch", "arity_mismatch")]
    if not hard:
        return True, None
    return False, hard[0]