import hashlib
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .xml_tools import parse_nifi_template

# ---------- Regex helpers
_SQL_DML_RE = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE)\b", re.IGNORECASE)
_SQL_DDL_META = re.compile(
    r"\b(REFRESH|RECOVER\s+PARTITIONS|ANALYZE|MSCK|SHOW|DESCRIBE)\b", re.IGNORECASE
)
_SQL_SELECT = re.compile(r"\bSELECT\b", re.IGNORECASE)
_MOVE_CMDS = re.compile(r"\b(mv|move)\b")
_CP_CMDS = re.compile(r"\b(cp|copy)\b")
_HDFS_CMDS = re.compile(r"\bhdfs(\s+dfs)?\b.*\b(-put|-mv|-cp|-rm)\b", re.IGNORECASE)
_IMPALA_SHELL = re.compile(r"\bimpala-shell\b", re.IGNORECASE)


def _get_prop_str(properties: Dict[str, Any]) -> str:
    try:
        return json.dumps(properties, ensure_ascii=False)
    except Exception:
        return str(properties)


def _any_property_matches(properties: Dict[str, Any], regex: re.Pattern) -> bool:
    return bool(regex.search(_get_prop_str(properties)))


def _extract_sql(properties: Dict[str, Any]) -> str:
    blob = _get_prop_str(properties)
    m = re.findall(r"-q;\"([^\"]+)\"", blob) + re.findall(
        r'"query[_\w]*"\s*:\s*"([^"]+)"', blob
    )
    return "\n".join(m)


def _parse_llm_json_simple(content: str) -> dict:
    """Simple JSON parsing with basic recovery."""
    # Try direct parsing first
    try:
        return json.loads(content.strip())
    except json.JSONDecodeError:
        pass

    # Try extracting from markdown code block
    if "```json" in content:
        try:
            json_part = content.split("```json")[1].split("```")[0].strip()
            return json.loads(json_part)
        except (json.JSONDecodeError, IndexError):
            pass

    # Final fallback - fail gracefully
    raise ValueError(f"Unable to parse JSON from LLM response")


# ---------- Deterministic rules WITH impact inline
def _classify_by_rules(
    processor_type: str, name: str, properties: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    pt = (processor_type or "").lower()
    nm = (name or "").lower()

    # Obvious movement processors
    if any(
        k in pt
        for k in ["listhdfs", "listfile", "fetchfile", "puthdfs", "putfile", "getfile"]
    ):
        return {
            "data_manipulation_type": "data_movement",
            "actual_data_processing": "Reads/writes or enumerates files without altering contents.",
            "transforms_data_content": False,
            "business_purpose": "Move/land files.",
            "data_impact_level": "low",
            "key_operations": ["file_list_or_io"],
        }

    # SplitContent -> treat as movement unless you implement record-aware logic elsewhere
    if "splitcontent" in pt:
        return {
            "data_manipulation_type": "data_movement",
            "actual_data_processing": "Splits files/streams into parts; no record-level content change.",
            "transforms_data_content": False,
            "business_purpose": "Chunk files for downstream processing.",
            "data_impact_level": "low",
            "key_operations": ["split_stream"],
        }

    # ExecuteStreamCommand: mv/cp/hdfs
    if "executestreamcommand" in pt:
        if (
            _any_property_matches(properties, _MOVE_CMDS)
            or _any_property_matches(properties, _CP_CMDS)
            or _any_property_matches(properties, _HDFS_CMDS)
        ):
            return {
                "data_manipulation_type": "data_movement",
                "actual_data_processing": "Moves/copies/deletes files; does not change file content.",
                "transforms_data_content": False,
                "business_purpose": "File lifecycle management (landing, quarantine, invalid bins).",
                "data_impact_level": "low",
                "key_operations": ["mv_cp_rm"],
            }
        # Impala shell detection
        if _any_property_matches(properties, _IMPALA_SHELL):
            sql = _extract_sql(properties)
            if _SQL_DML_RE.search(sql):
                return {
                    "data_manipulation_type": "external_processing",
                    "actual_data_processing": "Executes SQL DML via impala-shell that changes table content.",
                    "transforms_data_content": True,
                    "business_purpose": "Apply inserts/updates/deletes to downstream tables.",
                    "data_impact_level": "high",
                    "key_operations": ["sql_dml"],
                }
            if _SQL_DDL_META.search(sql) or _SQL_SELECT.search(sql):
                # Metadata/diagnostic only
                return {
                    "data_manipulation_type": "infrastructure_only",
                    "actual_data_processing": "Runs metadata/diagnostic SQL (REFRESH/RECOVER/ANALYZE/MSCK/SHOW/DESCRIBE/SELECT-only). No row content altered.",
                    "transforms_data_content": False,
                    "business_purpose": "Keep catalogs/partitions in sync or inspect state.",
                    "data_impact_level": "none",
                    "key_operations": ["sql_metadata"],
                }

    # UpdateAttribute: setting attributes or storing queries only
    if "updateattribute" in pt:
        return {
            "data_manipulation_type": "infrastructure_only",
            "actual_data_processing": "Sets FlowFile attributes / stores SQL strings; does not execute or change content.",
            "transforms_data_content": False,
            "business_purpose": "Prepare routing/config/SQL text for later processors.",
            "data_impact_level": "none",
            "key_operations": ["set_attributes"],
        }

    # Custom_* quick heuristics
    if nm.startswith("custom_") or "custom" in pt:
        if (
            any(tok in nm for tok in ["copy", "move"])
            or _any_property_matches(properties, _MOVE_CMDS)
            or _any_property_matches(properties, _CP_CMDS)
        ):
            return {
                "data_manipulation_type": "data_movement",
                "actual_data_processing": "Custom move/copy; no content change.",
                "transforms_data_content": False,
                "business_purpose": "File logistics.",
                "data_impact_level": "low",
                "key_operations": ["custom_mv_cp"],
            }
        if "linecount" in nm or "count_number_of_lines" in nm or "wc" in nm:
            return {
                "data_manipulation_type": "infrastructure_only",
                "actual_data_processing": "Computes counts/metrics; no content modification.",
                "transforms_data_content": False,
                "business_purpose": "Monitoring/validation.",
                "data_impact_level": "none",
                "key_operations": ["metrics"],
            }

    # No confident rule
    return None


# ---------- Final overrides WITH impact inline
def _final_overrides(
    res: Dict[str, Any], processor_type: str, name: str, properties: Dict[str, Any]
) -> Dict[str, Any]:
    ptype = (processor_type or "").lower()
    nm = (name or "").lower()

    # Movement signatures trump transformation
    if (
        _any_property_matches(properties, _MOVE_CMDS)
        or _any_property_matches(properties, _CP_CMDS)
        or _any_property_matches(properties, _HDFS_CMDS)
        or any(
            k in ptype
            for k in [
                "listhdfs",
                "listfile",
                "fetchfile",
                "puthdfs",
                "putfile",
                "getfile",
            ]
        )
        or "splitcontent" in ptype
    ):
        res.update(
            {
                "data_manipulation_type": "data_movement",
                "transforms_data_content": False,
                "data_impact_level": "low",
            }
        )
        return res

    # UpdateAttribute never transforms content by itself
    if "updateattribute" in ptype:
        res.update(
            {
                "data_manipulation_type": "infrastructure_only",
                "transforms_data_content": False,
                "data_impact_level": "none",
            }
        )
        return res

    # impala-shell metadata ops are not content-changing
    if _any_property_matches(properties, _IMPALA_SHELL):
        sql = _extract_sql(properties)
        if not _SQL_DML_RE.search(sql):
            res.update(
                {
                    "data_manipulation_type": "infrastructure_only",
                    "transforms_data_content": False,
                    "data_impact_level": "none",
                }
            )
            return res

    return res


# ---------- Main analysis: rules → LLM (flags) → overrides (all with impact inline)
def classify_processor_improved(
    processor_type: str,
    properties: Dict[str, Any],
    name: str,
    proc_id: str,
) -> Dict[str, Any]:
    """
    Hybrid: deterministic rules first; LLM only as a tiebreaker.
    Impact levels are assigned inline; no _determine_impact_level needed.
    """
    model_endpoint = os.environ.get(
        "MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
    )

    # 0) Try deterministic rules FIRST
    rule_hit = _classify_by_rules(processor_type, name, properties)
    if rule_hit:
        result = {
            **rule_hit,
            "processor_type": processor_type,
            "properties": properties,
            "id": proc_id,
            "name": name,
            "analysis_method": "rules_first",
        }
        print(f"✅ [RULES] Classified {name}: {result['data_manipulation_type']}")
        return result

    # 1) Fallback to LLM (flags)
    try:
        try:
            from databricks_langchain import ChatDatabricks
        except ImportError:
            try:
                from langchain_community.chat_models import ChatDatabricks
            except ImportError:
                raise ImportError("Databricks LLM not available")

        llm = ChatDatabricks(endpoint=model_endpoint, temperature=0.0)

        prompt = f"""You are a NiFi expert. Return ONLY compact JSON with decision flags.
Processor:
- Type: {processor_type}
- Name: {name}
- Properties: {json.dumps(properties, indent=2, ensure_ascii=False)}

Decide booleans STRICTLY:
- touches_flowfile_content: true/false
- executes_sql_here: true/false
- sql_has_dml: true/false
- sql_is_metadata_only: true/false
- moves_or_renames_files: true/false
- sets_only_attributes: true/false
- rule_id: short string naming the single rule you used

Then provide:
- data_manipulation_type: one of ["data_transformation","data_movement","infrastructure_only","external_processing"]

Return ONLY:
{{
  "touches_flowfile_content": <bool>,
  "executes_sql_here": <bool>,
  "sql_has_dml": <bool>,
  "sql_is_metadata_only": <bool>,
  "moves_or_renames_files": <bool>,
  "sets_only_attributes": <bool>,
  "rule_id": "<string>",
  "data_manipulation_type": "<label>"
}}
"""
        flags_raw = llm.invoke(prompt)
        flags = _parse_llm_json_simple(flags_raw.content.strip())

        # 2) Deterministic mapping FROM flags (with impact inline)
        if flags.get("moves_or_renames_files"):
            mapped = ("data_movement", False, "low", "File move/copy/delete.")
        elif flags.get("sets_only_attributes"):
            mapped = ("infrastructure_only", False, "none", "Sets attributes only.")
        elif flags.get("executes_sql_here") and flags.get("sql_has_dml"):
            mapped = (
                "external_processing",
                True,
                "high",
                "Executes SQL DML that changes table content.",
            )
        elif flags.get("executes_sql_here") and flags.get("sql_is_metadata_only"):
            mapped = (
                "infrastructure_only",
                False,
                "none",
                "Executes metadata-only SQL.",
            )
        elif flags.get("touches_flowfile_content"):
            mapped = (
                "data_transformation",
                True,
                "medium",
                "Transforms record/file content.",
            )
        else:
            mapped = (
                "infrastructure_only",
                False,
                "none",
                "No evidence of content changes.",
            )

        data_manipulation_type, transforms, impact, rationale = mapped

        analysis_result = {
            "data_manipulation_type": data_manipulation_type,
            "actual_data_processing": rationale,
            "transforms_data_content": transforms,
            "business_purpose": "See rationale.",
            "data_impact_level": impact,
            "key_operations": [flags.get("rule_id", "llm_fallback")],
            "processor_type": processor_type,
            "properties": properties,
            "id": proc_id,
            "name": name,
            "analysis_method": "rules_then_llm",
            "llm_flags": flags,
        }

        # 3) Final overrides (also set impact inline)
        analysis_result = _final_overrides(
            analysis_result, processor_type, name, properties
        )

        print(
            f"✅ [LLM] Classified {name}: {analysis_result['data_manipulation_type']} ({analysis_result.get('key_operations')})"
        )
        return analysis_result

    except Exception as e:
        print(f"❌ [HYBRID LLM] Analysis failed for {name}: {str(e)}")
        return {
            "processor_type": processor_type,
            "properties": properties,
            "id": proc_id,
            "name": name,
            "data_manipulation_type": "unknown",
            "actual_data_processing": f"Enhanced LLM analysis failed: {str(e)}",
            "transforms_data_content": False,
            "business_purpose": f"Unknown processor: {name}",
            "data_impact_level": "unknown",
            "key_operations": ["analysis_failed"],
            "analysis_method": "hybrid_llm_fallback",
            "error": str(e),
        }


# convenience wrapper functions with caching and summaries


def _props_key(props: Dict[str, Any]) -> str:
    """Generate cache key for properties to deduplicate identical processors."""
    s = json.dumps(props, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def analyze_processors_batch(processors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Optional convenience wrapper with de-dup caching."""
    cache: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    results: List[Dict[str, Any]] = []

    for p in processors:
        pt = p.get("type", "") or ""
        nm = p.get("name", "") or ""
        pid = p.get("id", "") or ""
        props = p.get("properties", {}) or {}

        # Cache key: processor type + properties hash + name
        key = (pt, _props_key(props), nm)

        if key not in cache:
            # Try rules first (fast, no LLM call)
            rule_result = _classify_by_rules(pt, nm, props)
            if rule_result:
                # Rules worked - add processor info and mark as rules_first
                cache[key] = {
                    **rule_result,
                    "processor_type": pt,
                    "properties": props,
                    "id": pid,
                    "name": nm,
                    "analysis_method": "rules_first",
                }
                print(
                    f"✅ [RULES] Classified {nm}: {rule_result['data_manipulation_type']}"
                )
            else:
                # Fallback to LLM (individual call for now)
                cache[key] = classify_processor_improved(
                    processor_type=pt, properties=props, name=nm, proc_id=pid
                )

        # Copy result and keep the actual id/name in case duplicates differed there
        r = dict(cache[key])
        r["id"] = pid
        r["name"] = nm
        results.append(r)

    print(
        f"🔄 [CACHE] Processed {len(processors)} processors, {len(cache)} unique classifications"
    )
    return results


def _summarize(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create summary statistics of classification results."""
    by_type = {
        "data_transformation": 0,
        "data_movement": 0,
        "infrastructure_only": 0,
        "external_processing": 0,
        "unknown": 0,
    }

    for r in results:
        classification = r.get("data_manipulation_type", "unknown")
        by_type[classification] = by_type.get(classification, 0) + 1

    return {
        "total": len(results),
        "by_type": by_type,
    }


def analyze_workflow_patterns(
    xml_path: str, save_markdown: bool = True, output_dir: str = None
) -> Dict[str, Any]:
    """Optional single entry-point: parse → classify → (optional) write JSON/MD."""
    try:
        # Read the XML file content first
        with open(xml_path, "r", encoding="utf-8") as f:
            xml_content = f.read()

        template_data = json.loads(parse_nifi_template(xml_content))
        processors = template_data.get("processors", [])
        classification_results = analyze_processors_batch(processors)
        summary = _summarize(classification_results)

        analysis_result = {
            "workflow_metadata": {
                "filename": os.path.basename(xml_path),
                "xml_path": xml_path,
                "total_processors": len(processors),
                "analysis_timestamp": datetime.now().isoformat(),
            },
            "summary": summary,
            "classification_results": classification_results,
        }

        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            base = os.path.splitext(os.path.basename(xml_path))[0]
            json_path = os.path.join(output_dir, f"{base}_workflow_analysis.json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(analysis_result, f, indent=2, ensure_ascii=False)

            if save_markdown:
                md_path = os.path.join(output_dir, f"{base}_workflow_analysis.md")
                with open(md_path, "w", encoding="utf-8") as f:
                    by = summary["by_type"]
                    f.write(
                        f"# Workflow Analysis: {base}\n\n"
                        f"- **Total processors:** {summary['total']}\n"
                        f"- **Data Transformation:** {by.get('data_transformation', 0)}\n"
                        f"- **Data Movement:** {by.get('data_movement', 0)}\n"
                        f"- **Infrastructure Only:** {by.get('infrastructure_only', 0)}\n"
                        f"- **External Processing:** {by.get('external_processing', 0)}\n"
                        f"- **Unknown:** {by.get('unknown', 0)}\n"
                    )

        print(f"📊 [SUMMARY] {summary['by_type']}")
        return analysis_result

    except Exception as e:
        return {
            "error": f"Failed to analyze workflow: {str(e)}",
            "workflow_metadata": {},
            "summary": {},
            "classification_results": [],
        }
