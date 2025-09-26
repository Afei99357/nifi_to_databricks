"""Aggregate per-group profiles from classifications."""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Sequence


def _normalise_group_path(record: Dict[str, Any]) -> str:
    """Return a stable group path for aggregation."""
    path = str(record.get("parent_group_path") or "").strip()
    if path:
        return path
    group = record.get("parent_group") or record.get("parentGroupName") or "Root"
    group = str(group).strip()
    return group or "Root"


def _leaf_name(path: str, fallback: str) -> str:
    """Return the most specific group label for display."""
    cleaned = path.strip("/")
    if not cleaned:
        return fallback or "Root"
    parts = cleaned.split("/")
    return parts[-1] or fallback or "Root"


def _init_profile(group: str, group_path: str) -> Dict[str, Any]:
    return {
        "group": group,
        "group_path": group_path,
        "processor_count": 0,
        "category_counts": Counter(),
        "needs_migration_count": 0,
        "ambiguous_count": 0,
        "inline_script_total": 0,
        "external_script_total": 0,
        "external_script_processors": set(),  # type: ignore[var-annotated]
        "variables_defined": set(),  # type: ignore[var-annotated]
        "variables_used": set(),  # type: ignore[var-annotated]
        "controller_services": set(),  # type: ignore[var-annotated]
        "incoming_groups": set(),  # type: ignore[var-annotated]
        "outgoing_groups": set(),  # type: ignore[var-annotated]
    }


def _extract_connection_ids(
    feature_evidence: Dict[str, Any],
) -> Tuple[List[str], List[str]]:
    connections = feature_evidence.get("connections", {}) or {}
    incoming = [str(value) for value in connections.get("incoming", []) or []]
    outgoing = [str(value) for value in connections.get("outgoing", []) or []]
    return incoming, outgoing


def build_group_profiles(
    classifications: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Aggregate processor-level classifications into group profiles."""

    id_to_group_path: Dict[str, str] = {}
    id_to_group_label: Dict[str, str] = {}

    for record in classifications:
        processor_id = record.get("processor_id") or record.get("id")
        if not processor_id:
            continue
        group_path = _normalise_group_path(record)
        group_label = record.get("parent_group") or _leaf_name(group_path, "Root")
        processor_id = str(processor_id)
        id_to_group_path[processor_id] = group_path
        id_to_group_label[processor_id] = str(group_label)

    profiles: Dict[str, Dict[str, Any]] = {}

    for record in classifications:
        processor_id = record.get("processor_id") or record.get("id")
        if not processor_id:
            continue
        processor_id = str(processor_id)

        group_path = id_to_group_path.get(processor_id) or _normalise_group_path(record)
        group_label = id_to_group_label.get(processor_id) or _leaf_name(
            group_path, "Root"
        )

        profile = profiles.setdefault(
            group_path, _init_profile(group_label, group_path)
        )

        profile["processor_count"] += 1

        category = record.get("migration_category") or "Ambiguous"
        profile["category_counts"][category] += 1
        if category != "Infrastructure Only":
            profile["needs_migration_count"] += 1

        confidence = float(record.get("confidence") or 0.0)
        if category == "Ambiguous" or confidence < 0.5:
            profile["ambiguous_count"] += 1

        feature_evidence = record.get("feature_evidence", {}) or {}
        scripts = feature_evidence.get("scripts", {}) or {}
        inline_count = int(scripts.get("inline_count") or 0)
        external_count = int(scripts.get("external_count") or 0)
        profile["inline_script_total"] += inline_count
        profile["external_script_total"] += external_count

        if external_count:
            profile["external_script_processors"].add(
                (processor_id, str(record.get("name") or ""))
            )

        variables = feature_evidence.get("variables", {}) or {}
        for definition in variables.get("defines", []) or []:
            var_name = definition.get("variable")
            if var_name:
                profile["variables_defined"].add(str(var_name))
        for usage in variables.get("uses", []) or []:
            var_name = usage.get("variable")
            if var_name:
                profile["variables_used"].add(str(var_name))

        for service in feature_evidence.get("controller_services", []) or []:
            if service:
                profile["controller_services"].add(str(service))

        incoming_ids, outgoing_ids = _extract_connection_ids(feature_evidence)
        for incoming_id in incoming_ids:
            incoming_group = id_to_group_path.get(incoming_id)
            if incoming_group and incoming_group != group_path:
                profile["incoming_groups"].add(incoming_group)
        for outgoing_id in outgoing_ids:
            outgoing_group = id_to_group_path.get(outgoing_id)
            if outgoing_group and outgoing_group != group_path:
                profile["outgoing_groups"].add(outgoing_group)

    results: List[Dict[str, Any]] = []
    for group_path, payload in profiles.items():
        category_counts = dict(sorted(payload["category_counts"].items()))
        dominant_category = None
        if category_counts:
            dominant_category = max(
                category_counts.items(), key=lambda item: (item[1], item[0])
            )[0]
        results.append(
            {
                "group": payload["group"],
                "group_path": group_path,
                "processor_count": payload["processor_count"],
                "migration_category_counts": category_counts,
                "dominant_category": dominant_category,
                "needs_migration_count": payload["needs_migration_count"],
                "ambiguous_count": payload["ambiguous_count"],
                "inline_script_total": payload["inline_script_total"],
                "external_script_total": payload["external_script_total"],
                "has_external_scripts": payload["external_script_total"] > 0,
                "external_script_processors": [
                    {"id": proc_id, "name": name}
                    for proc_id, name in sorted(
                        payload["external_script_processors"], key=lambda item: item[0]
                    )
                ],
                "variables_defined": sorted(payload["variables_defined"]),
                "variables_used": sorted(payload["variables_used"]),
                "controller_services": sorted(payload["controller_services"]),
                "incoming_groups": sorted(payload["incoming_groups"]),
                "outgoing_groups": sorted(payload["outgoing_groups"]),
                "incoming_group_count": len(payload["incoming_groups"]),
                "outgoing_group_count": len(payload["outgoing_groups"]),
            }
        )

    results.sort(key=lambda item: item["group_path"])
    return results


__all__ = ["build_group_profiles"]
