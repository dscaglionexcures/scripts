#!/usr/bin/env python3
"""
Evaluate a checklist for a subject and generate a polished PDF report.

Features:
- Calls POST /api/v1/patient-registry/checklist/{checklistId}/evaluate
- Resolves checklist display name from GET /api/v1/patient-registry/checklist
- Renders a sectioned PDF that adapts to arbitrary checklist item result schemas
- Optionally saves the raw evaluation JSON alongside the PDF
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
from xml.sax.saxutils import escape
from zoneinfo import ZoneInfo

import requests
from pypdf import PdfReader, PdfWriter
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from xcures_toolkit.api_common import require_json_list, require_json_object
from xcures_toolkit.auth_common import get_xcures_bearer_token, load_env_file
from xcures_toolkit.xcures_client import XcuresApiClient


DEFAULT_BASE_URL = "https://partner.xcures.com"
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = REPO_ROOT / "reports" / "checklist"
EVALUATE_ENDPOINT_TEMPLATE = "/api/v1/patient-registry/checklist/{checklist_id}/evaluate"
DEFAULT_TIMEOUT_SECONDS = 60


@dataclass(frozen=True)
class DetailRow:
    label: str
    value: str


@dataclass(frozen=True)
class ItemViewModel:
    title: str
    sort_order: float
    meets_criteria: bool
    details: List[DetailRow]
    result_raw: Optional[Dict[str, Any]]
    evidence: str
    source_lines: List[str]
    document_count: int
    result_is_structured: bool


@dataclass(frozen=True)
class ReportMeta:
    subject_id: str
    subject_full_name: str
    subject_external_id: str
    subject_dob: str
    checklist_id: str
    checklist_name: str
    eligibility_satisfied: bool
    generated_at: datetime
    total_items: int
    passed_items: int
    failed_items: int
    unique_document_count: int
    endpoint_path: str


@dataclass(frozen=True)
class RenderConfig:
    page_size: tuple[float, float]
    margin_left: float
    margin_right: float
    margin_top: float
    margin_bottom: float
    header_height: float
    footer_height: float
    accent_color: colors.Color
    header_bg_color: colors.Color
    pass_color: colors.Color
    fail_color: colors.Color
    border_color: colors.Color
    muted_text_color: colors.Color
    card_bg_color: colors.Color


def build_render_config() -> RenderConfig:
    return RenderConfig(
        page_size=letter,
        margin_left=0.7 * inch,
        margin_right=0.7 * inch,
        margin_top=1.9 * inch,
        margin_bottom=0.75 * inch,
        header_height=1.48 * inch,
        footer_height=0.55 * inch,
        accent_color=colors.HexColor("#0F4C81"),
        header_bg_color=colors.HexColor("#045591"),
        pass_color=colors.HexColor("#2E7D32"),
        fail_color=colors.HexColor("#B71C1C"),
        border_color=colors.HexColor("#B0BEC5"),
        muted_text_color=colors.HexColor("#546E7A"),
        card_bg_color=colors.HexColor("#F7FAFC"),
    )


def default_timeout_seconds() -> int:
    raw_value = (os.getenv("request_timeout_seconds", "") or "").strip()
    if not raw_value:
        return DEFAULT_TIMEOUT_SECONDS
    try:
        parsed = int(raw_value)
    except ValueError:
        return DEFAULT_TIMEOUT_SECONDS
    if parsed <= 0:
        return DEFAULT_TIMEOUT_SECONDS
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate a checklist for a subject and export a polished PDF report "
            "(optionally with raw JSON sidecar)."
        )
    )
    parser.add_argument(
        "--subject-id",
        required=False,
        help="Subject UUID to evaluate. If omitted, you will be prompted.",
    )
    parser.add_argument(
        "--checklist-id",
        required=False,
        help="Checklist UUID to evaluate. If omitted, you will pick from a numbered list.",
    )
    parser.add_argument(
        "--project-id",
        default=os.getenv("XCURES_PROJECT_ID", "").strip() or None,
        help="ProjectId header value (defaults to XCURES_PROJECT_ID env var).",
    )
    parser.add_argument(
        "--base-url",
        default=(
            os.getenv("XCURES_BASE_URL", "").strip()
            or os.getenv("BASE_URL", "").strip()
            or DEFAULT_BASE_URL
        ),
        help="API base URL.",
    )
    parser.add_argument(
        "--bearer",
        default=None,
        help="Optional bearer token override. If absent, uses client credentials flow.",
    )
    parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Force checklist re-evaluation even if cached results exist.",
    )
    parser.add_argument(
        "--save-json",
        dest="save_json",
        action="store_true",
        default=True,
        help="Save raw evaluation JSON next to the PDF output (default: enabled).",
    )
    parser.add_argument(
        "--no-save-json",
        dest="save_json",
        action="store_false",
        help="Disable saving raw evaluation JSON sidecar for this run.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Directory for generated files (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=default_timeout_seconds(),
        help="Request timeout in seconds (defaults to request_timeout_seconds env, else 60).",
    )
    args = parser.parse_args()
    if not args.project_id:
        parser.error("--project-id is required (or set XCURES_PROJECT_ID in the environment).")
    return args


def normalize_label(key: str) -> str:
    cleaned = key.replace("_", " ").replace("-", " ").strip()
    if not cleaned:
        return "Field"
    return " ".join(part.capitalize() for part in cleaned.split())


def _is_scalar(value: Any) -> bool:
    return not isinstance(value, (dict, list))


def _indent_multiline(text: str, prefix: str = "  ") -> str:
    lines = text.splitlines()
    if not lines:
        return text
    return "\n".join(f"{prefix}{line}" for line in lines)


def _scalar_to_text(value: Any) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _humanize_value(value: Any, *, depth: int = 0) -> str:
    if depth > 6:
        return "..."

    if _is_scalar(value):
        return _scalar_to_text(value)

    if isinstance(value, dict):
        if not value:
            return "N/A"

        lines: List[str] = []
        for key, nested_value in value.items():
            label = normalize_label(str(key))
            nested_text = _humanize_value(nested_value, depth=depth + 1)
            if "\n" in nested_text:
                lines.append(f"{label}:")
                lines.append(_indent_multiline(nested_text, "  "))
            else:
                lines.append(f"{label}: {nested_text}")
        return "\n".join(lines)

    # value is a list at this point
    if not value:
        return "N/A"

    if all(_is_scalar(item) for item in value):
        return ", ".join(_scalar_to_text(item) for item in value)

    bullet_lines: List[str] = []
    for item in value:
        nested_text = _humanize_value(item, depth=depth + 1)
        nested_parts = nested_text.splitlines() or [nested_text]
        bullet_lines.append(f"- {nested_parts[0]}")
        for line in nested_parts[1:]:
            bullet_lines.append(f"  {line}")
    return "\n".join(bullet_lines)


def value_to_text(value: Any) -> str:
    return _humanize_value(value)


def compact_id(identifier: str, max_len: int = 8) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "", identifier or "")
    if not cleaned:
        return "id"
    return cleaned[:max_len]


def html_paragraph_text(raw: str) -> str:
    safe = escape(raw or "")
    return safe.replace("\n", "<br/>")


def _bold_colon_label_in_line(line: str) -> str:
    # For human-facing key/value lines, bold only short label-like prefixes.
    # This avoids over-formatting prose that contains references like
    # "(demographic:5, social_history:6)".
    #
    # Examples matched:
    # - "Patient Instructions: take with food" -> "<b>Patient Instructions:</b> ..."
    # - "- Medication: aspirin 81 mg" -> "- <b>Medication:</b> ..."
    # - "Alcohol Use Status:" -> "<b>Alcohol Use Status:</b>"
    safe = escape(line)
    match = re.match(
        r"^(\s*(?:-\s+)?)"
        r"((?:[A-Za-z][A-Za-z0-9'()/.\-]*"
        r"(?:\s+[A-Za-z][A-Za-z0-9'()/.\-]*){0,5}):)"
        r"(\s*.*)$",
        safe,
    )
    if not match:
        return safe

    prefix, label, suffix = match.groups()
    return f"{prefix}<b>{label}</b>{suffix}"


def html_paragraph_text_with_colon_word_bold(raw: str) -> str:
    lines = (raw or "").splitlines()
    if not lines:
        return ""
    return "<br/>".join(_bold_colon_label_in_line(line) for line in lines)


def styled_paragraph(raw: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(html_paragraph_text(raw), style)


def styled_paragraph_with_colon_word_bold(raw: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(html_paragraph_text_with_colon_word_bold(raw), style)


def append_text_with_colon_headings(
    block: List[Any],
    text: str,
    *,
    body_style: ParagraphStyle,
) -> None:
    lines = (text or "").splitlines()
    if not lines:
        return

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            block.append(Spacer(1, 0.02 * inch))
            continue
        if ":" in stripped:
            block.append(styled_paragraph_with_colon_word_bold(line, body_style))
            if stripped.endswith(":"):
                block.append(Spacer(1, 0.05 * inch))
            continue
        block.append(styled_paragraph(line, body_style))


def extract_evidence(result: Any) -> str:
    if not isinstance(result, dict):
        return "No structured result provided."
    justification = result.get("justification")
    if isinstance(justification, str) and justification.strip():
        return justification.strip()
    evidence = result.get("evidence")
    if evidence is None:
        return "No evidence text provided."
    return value_to_text(evidence)


def build_source_lines(item: Dict[str, Any]) -> tuple[List[str], int]:
    lines: List[str] = []
    document_ids = item.get("documentIds")
    if not isinstance(document_ids, list):
        document_ids = []

    section_doc_ids: set[str] = set()
    sections = item.get("documentSections")
    if isinstance(sections, list) and sections:
        for section in sections:
            if not isinstance(section, dict):
                continue
            doc_id = str(section.get("documentId") or "").strip()
            if doc_id:
                section_doc_ids.add(doc_id)

            doc_name = str(section.get("documentName") or "Document").strip()
            doc_date = str(section.get("documentDate") or "date unknown").strip()
            section_type = str(section.get("sectionType") or "").strip()
            section_ref = str(section.get("section") or "").strip()
            section_title = str(section.get("sectionTitle") or "").strip()

            section_bits = [f"type={section_type or 'unknown'}"]
            if section_ref:
                section_bits.append(f"ref={section_ref}")
            if section_title and section_title.lower() != "none":
                section_bits.append(f"title={section_title}")

            lines.append(f"{doc_name} ({doc_date}) - {', '.join(section_bits)}")
    else:
        for doc_id in document_ids[:12]:
            lines.append(f"documentId={doc_id}")
        if len(document_ids) > 12:
            lines.append(f"... and {len(document_ids) - 12} more document IDs")

    unique_document_count = len(set(str(x) for x in document_ids if x) | section_doc_ids)
    if not lines:
        lines = ["No source references provided."]
    return lines, unique_document_count


def normalize_item(item: Dict[str, Any]) -> ItemViewModel:
    checklist_item = item.get("checklistItem")
    checklist_item_obj = checklist_item if isinstance(checklist_item, dict) else {}
    title = (
        str(checklist_item_obj.get("name") or "").strip()
        or str(checklist_item_obj.get("libraryItemId") or "").strip()
        or str(checklist_item_obj.get("id") or "").strip()
        or "Untitled checklist item"
    )

    raw_sort_order = checklist_item_obj.get("sortOrder")
    try:
        sort_order = float(raw_sort_order)
    except Exception:
        sort_order = 10_000.0

    meets_criteria = bool(item.get("meetsCriteria"))

    result = item.get("result")
    details: List[DetailRow] = []
    if isinstance(result, dict):
        for key, value in result.items():
            details.append(DetailRow(label=normalize_label(str(key)), value=value_to_text(value)))

    source_lines, doc_count = build_source_lines(item)
    return ItemViewModel(
        title=title,
        sort_order=sort_order,
        meets_criteria=meets_criteria,
        details=details,
        result_raw=result if isinstance(result, dict) else None,
        evidence=extract_evidence(result),
        source_lines=source_lines,
        document_count=doc_count,
        result_is_structured=isinstance(result, dict),
    )


def clean_section_title(title: str, checklist_name: str) -> str:
    raw_title = (title or "").strip()
    if not raw_title:
        return raw_title

    checklist = (checklist_name or "").strip()
    if checklist:
        prefix_pattern = re.compile(rf"^\s*{re.escape(checklist)}\s*-\s*", re.IGNORECASE)
        cleaned = prefix_pattern.sub("", raw_title, count=1).strip()
        if cleaned:
            return cleaned

    # Fallback for older checklist labels that still include "RECAP - ...".
    recap_cleaned = re.sub(r"^\s*RECAP\s*-\s*", "", raw_title, flags=re.IGNORECASE).strip()
    if recap_cleaned:
        return recap_cleaned

    return raw_title


def build_styles(config: RenderConfig) -> Dict[str, ParagraphStyle]:
    sample = getSampleStyleSheet()
    return {
        "h1": ParagraphStyle(
            "H1Custom",
            parent=sample["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=18,
            leading=21,
            textColor=colors.HexColor("#102A43"),
            spaceBefore=10,
            spaceAfter=8,
        ),
        "h2": ParagraphStyle(
            "H2Custom",
            parent=sample["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=14,
            leading=17,
            textColor=config.accent_color,
            spaceBefore=8,
            spaceAfter=6,
        ),
        "h3": ParagraphStyle(
            "H3Custom",
            parent=sample["Heading3"],
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=15,
            textColor=config.accent_color,
            spaceBefore=6,
            spaceAfter=5,
        ),
        "section": ParagraphStyle(
            "SectionHeading",
            parent=sample["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=14,
            textColor=config.accent_color,
            spaceBefore=8,
            spaceAfter=6,
        ),
        "body": ParagraphStyle(
            "BodyTextCustom",
            parent=sample["BodyText"],
            fontName="Helvetica",
            fontSize=9.5,
            leading=12.5,
            spaceBefore=1,
            spaceAfter=1,
        ),
        "small": ParagraphStyle(
            "SmallText",
            parent=sample["BodyText"],
            fontName="Helvetica",
            fontSize=8.3,
            leading=10.4,
            textColor=config.muted_text_color,
        ),
        "table_header": ParagraphStyle(
            "TableHeader",
            parent=sample["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=9,
            textColor=colors.white,
            leading=11,
        ),
    }


def draw_page_chrome(
    canvas_obj: Any,
    doc: Any,
    *,
    meta: ReportMeta,
    config: RenderConfig,
) -> None:
    def draw_label_value_line(
        *,
        x: float,
        y: float,
        label: str,
        value: str,
        label_font: str,
        value_font: str,
        font_size: float,
    ) -> None:
        label_text = f"{label}:"
        canvas_obj.setFont(label_font, font_size)
        canvas_obj.drawString(x, y, label_text)
        label_w = canvas_obj.stringWidth(label_text, label_font, font_size)
        canvas_obj.setFont(value_font, font_size)
        canvas_obj.drawString(x + label_w + 3, y, value)

    width, height = config.page_size
    generated_at_label = meta.generated_at.strftime("%Y-%m-%d %H:%M:%S %Z")

    canvas_obj.saveState()
    canvas_obj.setFillColor(config.header_bg_color)
    canvas_obj.rect(0, height - config.header_height, width, config.header_height, stroke=0, fill=1)

    canvas_obj.setFillColor(colors.white)
    canvas_obj.setFont("Helvetica-Bold", 13)
    canvas_obj.drawString(config.margin_left, height - 0.35 * inch, "Checklist Evaluation Report")

    canvas_obj.setFillColor(colors.white)
    draw_label_value_line(
        x=config.margin_left,
        y=height - 0.54 * inch,
        label="Checklist",
        value=meta.checklist_name,
        label_font="Helvetica-Bold",
        value_font="Helvetica",
        font_size=8.6,
    )
    draw_label_value_line(
        x=config.margin_left,
        y=height - 0.71 * inch,
        label="Patient",
        value=meta.subject_full_name,
        label_font="Helvetica-Bold",
        value_font="Helvetica",
        font_size=8.6,
    )
    draw_label_value_line(
        x=config.margin_left,
        y=height - 0.88 * inch,
        label="External ID",
        value=meta.subject_external_id,
        label_font="Helvetica-Bold",
        value_font="Helvetica",
        font_size=8.6,
    )
    draw_label_value_line(
        x=config.margin_left,
        y=height - 1.05 * inch,
        label="DOB",
        value=meta.subject_dob,
        label_font="Helvetica-Bold",
        value_font="Helvetica",
        font_size=8.6,
    )
    draw_label_value_line(
        x=config.margin_left,
        y=height - 1.22 * inch,
        label="Generated",
        value=generated_at_label,
        label_font="Helvetica-Bold",
        value_font="Helvetica",
        font_size=8.6,
    )

    footer_y = config.footer_height
    canvas_obj.setStrokeColor(config.border_color)
    canvas_obj.line(config.margin_left, footer_y, width - config.margin_right, footer_y)

    canvas_obj.setFillColor(config.muted_text_color)
    canvas_obj.setFont("Helvetica", 8.1)
    canvas_obj.drawRightString(width - config.margin_right, 0.35 * inch, f"Page {doc.page}")
    canvas_obj.restoreState()


def build_item_block(
    *,
    index: int,
    item: ItemViewModel,
    styles: Dict[str, ParagraphStyle],
    config: RenderConfig,
    usable_width: float,
) -> List[Any]:
    def key_normalize(raw: str) -> str:
        return re.sub(r"[^a-z0-9]", "", (raw or "").lower())

    def lookup_value(record: Dict[str, Any], *aliases: str) -> str:
        normalized = {key_normalize(k): v for k, v in record.items()}
        for alias in aliases:
            value = normalized.get(key_normalize(alias))
            if value is not None:
                return value_to_text(value)
        return "N/A"

    def maybe_medication_rows(raw_result: Optional[Dict[str, Any]]) -> Optional[List[List[str]]]:
        if not isinstance(raw_result, dict):
            return None
        raw_items: Any = None
        for candidate_key in ("items", "_items", "medications", "medication_items"):
            candidate_value = raw_result.get(candidate_key)
            if isinstance(candidate_value, list) and candidate_value:
                raw_items = candidate_value
                break

        # Fallback: first non-empty list of objects in the result payload.
        if raw_items is None:
            for candidate_value in raw_result.values():
                if (
                    isinstance(candidate_value, list)
                    and candidate_value
                    and all(isinstance(entry, dict) for entry in candidate_value)
                ):
                    raw_items = candidate_value
                    break

        if not isinstance(raw_items, list) or not raw_items:
            return None

        rows: List[List[str]] = []
        for entry in raw_items:
            if not isinstance(entry, dict):
                continue
            rows.append(
                [
                    lookup_value(entry, "medication"),
                    lookup_value(entry, "dose"),
                    lookup_value(entry, "formulation"),
                    lookup_value(entry, "patient_instructions", "patient instructions"),
                    lookup_value(entry, "reason"),
                ]
            )
        if not rows:
            return None
        return rows

    def _pick_items_list(raw_result: Optional[Dict[str, Any]], preferred_keys: Sequence[str]) -> Optional[List[Any]]:
        if not isinstance(raw_result, dict):
            return None

        for candidate_key in preferred_keys:
            candidate_value = raw_result.get(candidate_key)
            if isinstance(candidate_value, list) and candidate_value:
                return candidate_value

        for candidate_value in raw_result.values():
            if (
                isinstance(candidate_value, list)
                and candidate_value
                and all(isinstance(entry, dict) for entry in candidate_value)
            ):
                return candidate_value
        return None

    def maybe_family_history_rows(raw_result: Optional[Dict[str, Any]]) -> Optional[List[List[str]]]:
        raw_items = _pick_items_list(raw_result, ("_items", "items", "family_history", "familyHistory"))
        if not isinstance(raw_items, list) or not raw_items:
            return None

        rows: List[List[str]] = []
        parent_justification = "N/A"
        if isinstance(raw_result, dict):
            parent_justification = lookup_value(raw_result, "justification", "Justification")

        for entry in raw_items:
            if not isinstance(entry, dict):
                continue
            rows.append(
                [
                    lookup_value(entry, "condition"),
                    lookup_value(entry, "relationship", "relation"),
                    lookup_value(entry, "justification", "Justification")
                    if lookup_value(entry, "justification", "Justification") != "N/A"
                    else parent_justification,
                ]
            )
        if not rows:
            return None
        return rows

    def maybe_allergy_rows(raw_result: Optional[Dict[str, Any]]) -> Optional[List[List[str]]]:
        raw_items = _pick_items_list(raw_result, ("_items", "items", "allergies", "allergy_items"))
        if not isinstance(raw_items, list) or not raw_items:
            return None

        rows: List[List[str]] = []
        for entry in raw_items:
            if not isinstance(entry, dict):
                continue
            rows.append(
                [
                    lookup_value(entry, "allergen", "allergy", "substance"),
                    lookup_value(entry, "reaction", "Reaction"),
                    lookup_value(entry, "recorded_date", "recordedDate", "date_recorded"),
                ]
            )
        if not rows:
            return None
        return rows

    def maybe_surgical_history_rows(raw_result: Optional[Dict[str, Any]]) -> Optional[List[List[str]]]:
        raw_items = _pick_items_list(raw_result, ("_items", "items", "surgical_history", "surgicalHistory"))
        if not isinstance(raw_items, list) or not raw_items:
            return None

        rows: List[List[str]] = []
        for entry in raw_items:
            if not isinstance(entry, dict):
                continue
            rows.append(
                [
                    lookup_value(entry, "surgical_procedure", "procedure", "surgery"),
                    lookup_value(entry, "procedure_date", "date", "surgery_date"),
                    lookup_value(entry, "anatomic_location", "location"),
                    lookup_value(entry, "reason"),
                    lookup_value(entry, "notes", "note"),
                ]
            )
        if not rows:
            return None
        return rows

    def maybe_imaging_diagnostic_rows(raw_result: Optional[Dict[str, Any]]) -> Optional[List[List[str]]]:
        raw_items = _pick_items_list(
            raw_result,
            ("_items", "items", "imaging", "imaging_diagnostic_procedures", "diagnostic_procedures"),
        )
        if not isinstance(raw_items, list) or not raw_items:
            return None

        rows: List[List[str]] = []
        for entry in raw_items:
            if not isinstance(entry, dict):
                continue
            rows.append(
                [
                    lookup_value(entry, "diagnostic_procedure", "procedure"),
                    lookup_value(entry, "procedure_date", "date"),
                    lookup_value(entry, "anatomic_location", "location"),
                    lookup_value(entry, "reason"),
                    lookup_value(entry, "findings", "finding"),
                    lookup_value(entry, "impression"),
                ]
            )
        if not rows:
            return None
        return rows

    def maybe_vision_visit_rows(raw_result: Optional[Dict[str, Any]]) -> Optional[List[List[str]]]:
        raw_items = _pick_items_list(raw_result, ("_items", "items", "vision_visits", "visionVisits"))
        if not isinstance(raw_items, list) or not raw_items:
            return None

        rows: List[List[str]] = []
        parent_justification = "N/A"
        if isinstance(raw_result, dict):
            parent_justification = lookup_value(raw_result, "justification", "Justification")

        for entry in raw_items:
            if not isinstance(entry, dict):
                continue
            entry_justification = lookup_value(entry, "justification", "Justification")
            rows.append(
                [
                    lookup_value(entry, "visit_date", "date"),
                    lookup_value(entry, "vision_visit", "visit"),
                    lookup_value(entry, "notes", "note"),
                    entry_justification if entry_justification != "N/A" else parent_justification,
                ]
            )
        if not rows:
            return None
        return rows

    def maybe_dental_visit_rows(raw_result: Optional[Dict[str, Any]]) -> Optional[List[List[str]]]:
        raw_items = _pick_items_list(raw_result, ("_items", "items", "dental_visits", "dentalVisits"))
        if not isinstance(raw_items, list) or not raw_items:
            return None

        rows: List[List[str]] = []
        for entry in raw_items:
            if not isinstance(entry, dict):
                continue
            rows.append(
                [
                    lookup_value(entry, "visit_date", "date"),
                    lookup_value(entry, "dental_visit", "visit"),
                    lookup_value(entry, "notes", "note"),
                ]
            )
        if not rows:
            return None
        return rows

    def maybe_immunization_rows(raw_result: Optional[Dict[str, Any]]) -> Optional[List[List[str]]]:
        raw_items = _pick_items_list(raw_result, ("_items", "items", "immunizations", "vaccinations"))
        if not isinstance(raw_items, list) or not raw_items:
            return None

        rows: List[List[str]] = []
        for entry in raw_items:
            if not isinstance(entry, dict):
                continue
            rows.append(
                [
                    lookup_value(entry, "immunization", "vaccine", "vaccination"),
                    lookup_value(entry, "immunization_date", "date", "date_administered"),
                ]
            )
        if not rows:
            return None
        return rows

    def build_custom_table(headers: List[str], rows: List[List[str]], col_widths: List[float]) -> Table:
        table_data: List[List[Any]] = [
            [
                Paragraph(f"<font color='#FFFFFF'><b>{html_paragraph_text(h)}</b></font>", styles["small"])
                for h in headers
            ]
        ]
        for row in rows:
            table_data.append([styled_paragraph(cell, styles["body"]) for cell in row])

        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#045591")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#FFFFFF")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#000000")),
                    ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#000000")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )
        return table

    def build_medications_table(rows: List[List[str]]) -> Table:
        headers = ["Medication", "Dose", "Formulation", "Patient Instructions", "Reason"]
        col_widths = [
            usable_width * 0.18,
            usable_width * 0.12,
            usable_width * 0.17,
            usable_width * 0.33,
            usable_width * 0.20,
        ]
        return build_custom_table(headers, rows, col_widths)

    def build_family_history_table(rows: List[List[str]]) -> Table:
        headers = ["Condition", "Relationship", "Justification"]
        col_widths = [
            usable_width * 0.22,
            usable_width * 0.18,
            usable_width * 0.60,
        ]
        return build_custom_table(headers, rows, col_widths)

    def build_allergies_table(rows: List[List[str]]) -> Table:
        headers = ["Allergen", "Reaction", "Recorded Date"]
        col_widths = [
            usable_width * 0.30,
            usable_width * 0.45,
            usable_width * 0.25,
        ]
        return build_custom_table(headers, rows, col_widths)

    def build_surgical_history_table(rows: List[List[str]]) -> Table:
        headers = [
            "Surgical Procedure",
            "Procedure Date",
            "Anatomic Location",
            "Reason",
            "Notes",
        ]
        col_widths = [
            usable_width * 0.22,
            usable_width * 0.14,
            usable_width * 0.17,
            usable_width * 0.20,
            usable_width * 0.27,
        ]
        return build_custom_table(headers, rows, col_widths)

    def build_imaging_diagnostic_table(rows: List[List[str]]) -> Table:
        headers = [
            "Diagnostic Procedure",
            "Procedure Date",
            "Anatomic Location",
            "Reason",
            "Findings",
            "Impression",
        ]
        col_widths = [
            usable_width * 0.17,
            usable_width * 0.12,
            usable_width * 0.14,
            usable_width * 0.12,
            usable_width * 0.22,
            usable_width * 0.23,
        ]
        return build_custom_table(headers, rows, col_widths)

    def build_vision_visits_table(rows: List[List[str]]) -> Table:
        headers = ["Visit Date", "Vision Visit", "Notes", "Justification"]
        col_widths = [
            usable_width * 0.16,
            usable_width * 0.20,
            usable_width * 0.24,
            usable_width * 0.40,
        ]
        return build_custom_table(headers, rows, col_widths)

    def build_dental_visits_table(rows: List[List[str]]) -> Table:
        headers = ["Visit Date", "Dental Visit", "Notes"]
        col_widths = [
            usable_width * 0.20,
            usable_width * 0.28,
            usable_width * 0.52,
        ]
        return build_custom_table(headers, rows, col_widths)

    def build_immunizations_table(rows: List[List[str]]) -> Table:
        headers = ["Immunization", "Immunization Date"]
        col_widths = [
            usable_width * 0.72,
            usable_width * 0.28,
        ]
        return build_custom_table(headers, rows, col_widths)

    title = Paragraph(f"{html_paragraph_text(item.title)}", styles["h1"])
    block: List[Any] = [title, Spacer(1, 0.05 * inch)]

    medications_rows = maybe_medication_rows(item.result_raw) if "medication" in item.title.lower() else None
    family_history_rows = (
        maybe_family_history_rows(item.result_raw) if "family history" in item.title.lower() else None
    )
    allergies_rows = maybe_allergy_rows(item.result_raw) if "allerg" in item.title.lower() else None
    surgical_history_rows = (
        maybe_surgical_history_rows(item.result_raw) if "surgical history" in item.title.lower() else None
    )
    imaging_diagnostic_rows = (
        maybe_imaging_diagnostic_rows(item.result_raw)
        if "imaging and diagnostic procedures" in item.title.lower()
        else None
    )
    vision_visit_rows = (
        maybe_vision_visit_rows(item.result_raw) if "vision visit" in item.title.lower() else None
    )
    dental_visit_rows = (
        maybe_dental_visit_rows(item.result_raw) if "dental visit" in item.title.lower() else None
    )
    immunization_rows = (
        maybe_immunization_rows(item.result_raw) if "immunization" in item.title.lower() else None
    )
    if medications_rows:
        block.append(Paragraph("Result Details", styles["h3"]))
        block.append(build_medications_table(medications_rows))
    elif immunization_rows:
        block.append(Paragraph("Result Details", styles["h3"]))
        block.append(build_immunizations_table(immunization_rows))
    elif dental_visit_rows:
        block.append(Paragraph("Result Details", styles["h3"]))
        block.append(build_dental_visits_table(dental_visit_rows))
    elif vision_visit_rows:
        block.append(Paragraph("Result Details", styles["h3"]))
        block.append(build_vision_visits_table(vision_visit_rows))
    elif imaging_diagnostic_rows:
        block.append(Paragraph("Result Details", styles["h3"]))
        block.append(build_imaging_diagnostic_table(imaging_diagnostic_rows))
    elif surgical_history_rows:
        block.append(Paragraph("Result Details", styles["h3"]))
        block.append(build_surgical_history_table(surgical_history_rows))
    elif allergies_rows:
        block.append(Paragraph("Result Details", styles["h3"]))
        block.append(build_allergies_table(allergies_rows))
    elif family_history_rows:
        block.append(Paragraph("Result Details", styles["h3"]))
        block.append(build_family_history_table(family_history_rows))
    elif item.result_is_structured and item.details:
        block.append(Paragraph("Result Details", styles["h3"]))
        for row in item.details:
            block.append(styled_paragraph_with_colon_word_bold(f"{row.label}:", styles["body"]))
            block.append(Spacer(1, 0.05 * inch))
            append_text_with_colon_headings(
                block,
                row.value,
                body_style=styles["body"],
            )
            block.append(Spacer(1, 0.03 * inch))
    else:
        block.append(styled_paragraph("No structured result provided.", styles["body"]))

    block.extend(
        [
            Spacer(1, 0.07 * inch),
            Paragraph("Evidence", styles["h3"]),
        ]
    )
    append_text_with_colon_headings(
        block,
        item.evidence,
        body_style=styles["body"],
    )

    divider = Table([[""]], colWidths=[usable_width])
    divider.setStyle(
        TableStyle(
            [
                ("LINEBELOW", (0, 0), (-1, -1), 0.6, config.border_color),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )
    block.extend([Spacer(1, 0.08 * inch), divider, Spacer(1, 0.14 * inch)])
    return block


def write_pdf_report(
    *,
    meta: ReportMeta,
    items: Sequence[ItemViewModel],
    output_pdf: Path,
    config: RenderConfig,
) -> None:
    styles = build_styles(config)
    doc = SimpleDocTemplate(
        str(output_pdf),
        pagesize=config.page_size,
        leftMargin=config.margin_left,
        rightMargin=config.margin_right,
        topMargin=config.margin_top,
        bottomMargin=config.margin_bottom,
        title="Checklist Evaluation Report",
        author="xCures Checklist Export",
    )

    usable_width = config.page_size[0] - config.margin_left - config.margin_right
    story: List[Any] = []
    if not items:
        story.append(Paragraph("No checklist items returned.", styles["body"]))
    else:
        for idx, item in enumerate(items, start=1):
            story.extend(
                build_item_block(
                    index=idx,
                    item=item,
                    styles=styles,
                    config=config,
                    usable_width=usable_width,
                )
            )

    def _on_page(canvas_obj: Any, doc_obj: Any) -> None:
        draw_page_chrome(canvas_obj, doc_obj, meta=meta, config=config)

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)


def attach_file_to_pdf(pdf_path: Path, attachment_path: Path) -> None:
    reader = PdfReader(str(pdf_path))
    writer = PdfWriter()
    for page in reader.pages:
        writer.add_page(page)
    if reader.metadata:
        writer.add_metadata(reader.metadata)

    with attachment_path.open("rb") as attachment_file:
        writer.add_attachment(attachment_path.name, attachment_file.read())

    temp_pdf_path = pdf_path.with_name(pdf_path.stem + ".tmp.pdf")
    with temp_pdf_path.open("wb") as out_file:
        writer.write(out_file)
    temp_pdf_path.replace(pdf_path)


def checklist_sort_key(item: Dict[str, Any]) -> tuple[float, str]:
    raw_sort_order = item.get("sortOrder")
    try:
        sort_order = float(raw_sort_order)
    except Exception:
        sort_order = 10_000.0
    display_name = str(item.get("name") or item.get("id") or "").strip().lower()
    return sort_order, display_name


def fetch_checklist_catalog(client: XcuresApiClient) -> List[Dict[str, Any]]:
    payload = client.request_json("GET", "/api/v1/patient-registry/checklist")
    candidates: List[Any]
    if isinstance(payload, list):
        candidates = payload
    elif isinstance(payload, dict):
        results = payload.get("results")
        if isinstance(results, list):
            candidates = results
        else:
            candidates = []
    else:
        raise RuntimeError(f"Unexpected checklist catalog payload type: {type(payload)}")

    checklists: List[Dict[str, Any]] = []
    for idx, item in enumerate(candidates):
        if not isinstance(item, dict):
            raise RuntimeError(f"Checklist catalog entry[{idx}] is not an object: {type(item)}")
        checklists.append(item)

    checklists.sort(key=checklist_sort_key)
    return checklists


def checklist_display_name(item: Dict[str, Any]) -> str:
    name = str(item.get("name") or "").strip()
    if name:
        return name
    item_id = str(item.get("id") or "").strip()
    return item_id or "Unnamed Checklist"


def resolve_checklist_name(checklists: Sequence[Dict[str, Any]], checklist_id: str) -> str:
    for item in checklists:
        if str(item.get("id") or "").strip() == checklist_id:
            return checklist_display_name(item)
    return checklist_id


def get_subject_header_info(client: XcuresApiClient, subject_id: str) -> tuple[str, str, str]:
    payload = client.request_json("GET", f"/api/v1/patient-registry/subject/{subject_id}")
    subject_obj = require_json_object(payload, context="Subject")
    first_name = str(subject_obj.get("firstName") or "").strip()
    last_name = str(subject_obj.get("lastName") or "").strip()
    full_name = " ".join(part for part in [first_name, last_name] if part).strip()
    if not full_name:
        full_name = subject_id

    external_id = "N/A"
    raw_external_ids = subject_obj.get("externalIdentifiers")
    if isinstance(raw_external_ids, list):
        for entry in raw_external_ids:
            if not isinstance(entry, dict):
                continue
            value = str(entry.get("externalIdentifier") or "").strip()
            if value:
                external_id = value
                break

    raw_dob = str(subject_obj.get("birthDate") or "").strip()
    dob = "N/A"
    if raw_dob:
        dob_date_str = raw_dob[:10]
        try:
            dob_date = date.fromisoformat(dob_date_str)
            today = datetime.now(ZoneInfo("America/New_York")).date()
            age = today.year - dob_date.year - (
                (today.month, today.day) < (dob_date.month, dob_date.day)
            )
            age_label = "Year" if age == 1 else "Years"
            dob = f"{dob_date_str} ({age} {age_label} Old)"
        except ValueError:
            dob = dob_date_str
    return full_name, external_id, dob


def prompt_subject_id(initial_subject_id: Optional[str]) -> str:
    if isinstance(initial_subject_id, str) and initial_subject_id.strip():
        return initial_subject_id.strip()

    while True:
        value = input("Enter subjectId: ").strip()
        if value:
            return value
        print("subjectId is required.")


def prompt_checklist_selection(
    checklists: Sequence[Dict[str, Any]],
    initial_checklist_id: Optional[str],
) -> tuple[str, str]:
    if isinstance(initial_checklist_id, str) and initial_checklist_id.strip():
        selected_id = initial_checklist_id.strip()
        return selected_id, resolve_checklist_name(checklists, selected_id)

    if not checklists:
        raise RuntimeError("No checklists available for interactive selection.")

    print("\nAvailable checklists:")
    for idx, item in enumerate(checklists, start=1):
        print(f"{idx} - {checklist_display_name(item)}")

    while True:
        raw = input("Select checklist number: ").strip()
        try:
            selected_idx = int(raw)
        except ValueError:
            print("Please enter a valid number.")
            continue
        if selected_idx < 1 or selected_idx > len(checklists):
            print(f"Please enter a number between 1 and {len(checklists)}.")
            continue

        selected = checklists[selected_idx - 1]
        selected_id = str(selected.get("id") or "").strip()
        if not selected_id:
            print("Selected checklist has no id. Please choose a different checklist.")
            continue
        return selected_id, checklist_display_name(selected)


def compute_unique_document_count(payload: Dict[str, Any], items: Sequence[Dict[str, Any]]) -> int:
    base_docs: set[str] = set()
    doc_ids = payload.get("documentIds")
    if isinstance(doc_ids, list):
        base_docs = {str(doc_id) for doc_id in doc_ids if str(doc_id).strip()}

    for item in items:
        item_doc_ids = item.get("documentIds")
        if isinstance(item_doc_ids, list):
            for doc_id in item_doc_ids:
                doc_str = str(doc_id).strip()
                if doc_str:
                    base_docs.add(doc_str)

        sections = item.get("documentSections")
        if isinstance(sections, list):
            for section in sections:
                if not isinstance(section, dict):
                    continue
                doc_str = str(section.get("documentId") or "").strip()
                if doc_str:
                    base_docs.add(doc_str)

    return len(base_docs)


def evaluate_checklist(
    *,
    client: XcuresApiClient,
    checklist_id: str,
    subject_id: str,
    regenerate: bool,
) -> Dict[str, Any]:
    body: Dict[str, Any] = {"subjectId": subject_id}
    if regenerate:
        body["regenerate"] = True
    endpoint = EVALUATE_ENDPOINT_TEMPLATE.format(checklist_id=checklist_id)
    payload = client.request_json("POST", endpoint, json_body=body)
    payload_obj = require_json_object(payload, context="ChecklistEvaluationResult")
    require_json_list(payload_obj.get("items"), context="ChecklistEvaluationResult.items")
    return payload_obj


def build_output_paths(
    *,
    output_dir: Path,
    subject_id: str,
    checklist_id: str,
    generated_at: datetime,
) -> tuple[Path, Path]:
    timestamp = generated_at.strftime("%Y%m%d_%H%M%S")
    base_name = (
        f"checklist_eval_{compact_id(subject_id)}_{compact_id(checklist_id)}_{timestamp}"
    )
    return output_dir / f"{base_name}.pdf", output_dir / f"{base_name}.json"


def main() -> int:
    load_env_file(REPO_ROOT / ".env")
    args = parse_args()

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    bearer = args.bearer.strip() if isinstance(args.bearer, str) and args.bearer.strip() else None
    if not bearer:
        bearer = get_xcures_bearer_token(timeout_seconds=args.timeout)

    config = build_render_config()
    now = datetime.now(ZoneInfo("America/New_York"))

    with requests.Session() as session:
        client = XcuresApiClient(
            session=session,
            base_url=str(args.base_url).rstrip("/"),
            project_id=args.project_id,
            bearer_token=bearer,
            timeout_seconds=args.timeout,
        )

        checklist_catalog: List[Dict[str, Any]]
        try:
            checklist_catalog = fetch_checklist_catalog(client)
        except Exception as exc:
            if not args.checklist_id:
                raise RuntimeError(
                    "Failed to load checklist catalog for interactive selection. "
                    "Provide --checklist-id to bypass selection."
                ) from exc
            print(
                f"Warning: failed to retrieve checklist catalog; using checklist ID as title ({exc})",
                file=sys.stderr,
            )
            checklist_catalog = []

        subject_id = prompt_subject_id(args.subject_id)
        subject_full_name = subject_id
        subject_external_id = "N/A"
        subject_dob = "N/A"
        try:
            subject_full_name, subject_external_id, subject_dob = get_subject_header_info(
                client, subject_id
            )
        except Exception as exc:
            print(
                "Warning: failed to retrieve subject header info; "
                f"using fallbacks ({exc})",
                file=sys.stderr,
            )

        checklist_id, checklist_name = prompt_checklist_selection(
            checklist_catalog,
            args.checklist_id,
        )

        evaluation = evaluate_checklist(
            client=client,
            checklist_id=checklist_id,
            subject_id=subject_id,
            regenerate=bool(args.regenerate),
        )

    raw_items = require_json_list(evaluation.get("items"), context="ChecklistEvaluationResult.items")
    typed_items: List[Dict[str, Any]] = []
    for idx, item in enumerate(raw_items):
        if not isinstance(item, dict):
            raise RuntimeError(
                f"ChecklistEvaluationResult.items[{idx}] must be an object; got {type(item)}"
            )
        typed_items.append(item)
    normalized_items = [normalize_item(item) for item in typed_items]
    normalized_items = [
        ItemViewModel(
            title=clean_section_title(item.title, checklist_name),
            sort_order=item.sort_order,
            meets_criteria=item.meets_criteria,
            details=item.details,
            result_raw=item.result_raw,
            evidence=item.evidence,
            source_lines=item.source_lines,
            document_count=item.document_count,
            result_is_structured=item.result_is_structured,
        )
        for item in normalized_items
    ]
    normalized_items.sort(key=lambda item: (item.sort_order, item.title.lower()))

    total_items = len(normalized_items)
    passed_items = sum(1 for item in normalized_items if item.meets_criteria)
    failed_items = total_items - passed_items
    unique_document_count = compute_unique_document_count(evaluation, typed_items)
    eligibility_satisfied = bool(evaluation.get("eligibilitySatisfied"))

    meta = ReportMeta(
        subject_id=subject_id,
        subject_full_name=subject_full_name,
        subject_external_id=subject_external_id,
        subject_dob=subject_dob,
        checklist_id=checklist_id,
        checklist_name=checklist_name,
        eligibility_satisfied=eligibility_satisfied,
        generated_at=now,
        total_items=total_items,
        passed_items=passed_items,
        failed_items=failed_items,
        unique_document_count=unique_document_count,
        endpoint_path="POST /api/v1/patient-registry/checklist/{checklistId}/evaluate",
    )

    pdf_path, json_path = build_output_paths(
        output_dir=output_dir,
        subject_id=subject_id,
        checklist_id=checklist_id,
        generated_at=now,
    )

    write_pdf_report(meta=meta, items=normalized_items, output_pdf=pdf_path, config=config)

    if args.save_json:
        with json_path.open("w", encoding="utf-8") as out_file:
            json.dump(evaluation, out_file, indent=2, ensure_ascii=False, sort_keys=True)
        attach_file_to_pdf(pdf_path, json_path)

    print(f"Wrote PDF: {pdf_path}")
    if args.save_json:
        print(f"Wrote JSON: {json_path}")
        print("Attached JSON to PDF as embedded file.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
