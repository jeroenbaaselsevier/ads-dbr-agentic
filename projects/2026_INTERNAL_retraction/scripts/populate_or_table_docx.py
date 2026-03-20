#!/usr/bin/env python3
"""Populate Table XXX Word templates from OR table CSV outputs.

Usage example:
    .venv/bin/python projects/2026_INTERNAL_retraction/scripts/populate_or_table_docx.py \
        --template "projects/2026_INTERNAL_retraction/Table XXX.docx" \
        --csv-base projects/2026_INTERNAL_retraction/output/retraction_or_tables_paperyears_20260319 \
        --output-dir projects/2026_INTERNAL_retraction/output \
        --suffix paperyears
"""

from __future__ import annotations

import argparse
import csv
import glob
import re
from copy import deepcopy
from pathlib import Path

from docx import Document
from docx.oxml import OxmlElement
from docx.shared import Pt

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"


def read_or_rows(pattern: str) -> dict[tuple[str, str], dict[str, str]]:
    rows: dict[tuple[str, str], dict[str, str]] = {}
    for file_path in sorted(glob.glob(pattern)):
        with open(file_path, encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                if row.get("variable") == "variable":
                    continue
                key = (row["variable"].strip(), row["level"].strip())
                rows[key] = row
    return rows


def format_ci(ci_str: str) -> str:
    m = re.match(r"\[([\d.]+),\s*([\d.]+)\]", ci_str.strip())
    if not m:
        return ci_str
    lower = float(m.group(1))
    upper = float(m.group(2))
    return f"{lower:.2f}-{upper:.2f}"


def format_or(or_str: str) -> str:
    if not or_str or or_str == "Ref":
        return or_str
    try:
        return f"{float(or_str):.2f}"
    except ValueError:
        return or_str


def get_caption_rprs(template_doc: Document):
    para = next(p for p in template_doc.paragraphs if p.text.strip())
    bold_rpr = None
    nonbold_rpr = None
    for r in para._element.findall(f"{{{W_NS}}}r"):
        rpr = r.find(f"{{{W_NS}}}rPr")
        if rpr is None:
            continue
        if rpr.find(f"{{{W_NS}}}b") is not None and bold_rpr is None:
            bold_rpr = deepcopy(rpr)
        if rpr.find(f"{{{W_NS}}}b") is None and nonbold_rpr is None:
            nonbold_rpr = deepcopy(rpr)
    return bold_rpr, nonbold_rpr


def _append_caption_run(p, text: str, rpr) -> None:
    if not text:
        return
    r_el = OxmlElement("w:r")
    if rpr is not None:
        r_el.append(deepcopy(rpr))
    t_el = OxmlElement("w:t")
    t_el.text = text
    t_el.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
    r_el.append(t_el)
    p._element.append(r_el)


def set_caption(doc: Document, caption_text: str, bold_rpr, nonbold_rpr) -> None:
    bold_prefix = "Table XXX."
    for p in doc.paragraphs:
        if p.text.strip():
            for child in list(p._element):
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if tag != "pPr":
                    p._element.remove(child)
            if caption_text.startswith(bold_prefix):
                _append_caption_run(p, bold_prefix, bold_rpr)
                _append_caption_run(p, caption_text[len(bold_prefix):], nonbold_rpr)
            else:
                _append_caption_run(p, caption_text, nonbold_rpr)
            break


def clear_cell(cell, text: str) -> None:
    if not cell.paragraphs:
        p = cell.add_paragraph()
    else:
        p = cell.paragraphs[0]

    # Remove all runs from all paragraphs in this cell, then create one run.
    for para in cell.paragraphs:
        for run in list(para.runs):
            run._element.getparent().remove(run._element)

    run = p.add_run(text)
    run.bold = False
    run.font.name = "Times New Roman"
    run.font.size = Pt(11)


def fill_row(table, row_idx: int, u_or: str, u_ci: str, m_or: str, m_ci: str) -> None:
    row = table.rows[row_idx]
    clear_cell(row.cells[1], u_or)
    clear_cell(row.cells[2], u_ci)
    clear_cell(row.cells[4], m_or)
    clear_cell(row.cells[5], m_ci)


def get_vals(data: dict[tuple[str, str], dict[str, str]], variable: str, level: str):
    row = data.get((variable, level), {})
    if not row:
        return "", "", "", ""
    u_or = format_or(row.get("univ_or", ""))
    u_ci = format_ci(row.get("univ_95ci", "")) if row.get("univ_95ci") else ""
    m_or = format_or(row.get("multiv_or", ""))
    m_ci = format_ci(row.get("multiv_95ci", "")) if row.get("multiv_95ci") else ""
    if u_or == "Ref":
        u_ci = ""
    if m_or == "Ref":
        m_ci = ""
    return u_or, u_ci, m_or, m_ci


def build_doc(
    template_path: Path,
    out_path: Path,
    data: dict[tuple[str, str], dict[str, str]],
    include_top_cited_row: bool,
    pub_label: str,
    caption: str,
) -> None:
    template_doc = Document(str(template_path))
    bold_rpr, nonbold_rpr = get_caption_rprs(template_doc)
    doc = Document(str(template_path))
    table = doc.tables[0]

    mappings = [
        (4, "Gender", "Men"),
        (9, "Age career (year of first publication)", "1992-2001"),
        (10, "Age career (year of first publication)", "2002-2011"),
        (11, "Age career (year of first publication)", ">=2012"),
        (15, "Income level", "All other income levels"),
        (16, "Income level", "Unknown"),
        (18, pub_label, "Per +1 unit"),
        (25, "Scientific field", "Agriculture, Fisheries & Forestry"),
        (26, "Scientific field", "Biology"),
        (27, "Scientific field", "Biomedical Research"),
        (28, "Scientific field", "Built Environment & Design"),
        (29, "Scientific field", "Chemistry"),
        (31, "Scientific field", "Communication & Textual Studies"),
        (32, "Scientific field", "Earth & Environmental Sciences"),
        (33, "Scientific field", "Economics & Business"),
        (34, "Scientific field", "Enabling & Strategic Technologies"),
        (35, "Scientific field", "Engineering"),
        (36, "Scientific field", "Historical Studies"),
        (37, "Scientific field", "Information & Communication Technologies"),
        (38, "Scientific field", "Mathematics & Statistics"),
        (39, "Scientific field", "Philosophy & Theology"),
        (40, "Scientific field", "Physics & Astronomy"),
        (41, "Scientific field", "Psychology & Cognitive Sciences"),
        (42, "Scientific field", "Public Health & Health Services"),
        (43, "Scientific field", "Social Sciences"),
        (44, "Scientific field", "Visual & Performing Arts"),
        (47, "Interaction", "Men in youngest cohort>=2012"),
    ]
    if include_top_cited_row:
        mappings.insert(7, (22, "Top-cited status", "Yes"))

    for row_idx, var, lvl in mappings:
        u_or, u_ci, m_or, m_ci = get_vals(data, var, lvl)
        fill_row(table, row_idx, u_or, u_ci, m_or, m_ci)

    # Clear Top-cited status reference row when not applicable (top-cited cohort only)
    if not include_top_cited_row:
        # Row 21 is the "Top-cited status / No" reference row in the template
        for cell in table.rows[21].cells:
            clear_cell(cell, "")

    # Keep template row but align label text to chosen exposure definition.
    clear_cell(table.rows[18].cells[0], pub_label)

    set_caption(doc, caption, bold_rpr, nonbold_rpr)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(out_path))


def infer_pub_label(all_data: dict[tuple[str, str], dict[str, str]]) -> str:
    pub_keys = [k for k in all_data if k[0].startswith("Publication volume")]
    if not pub_keys:
        return "Publication volume (per paper)"
    return pub_keys[0][0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", required=True)
    parser.add_argument("--csv-base", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--suffix", default="")
    args = parser.parse_args()

    template = Path(args.template)
    base = Path(args.csv_base)
    out_dir = Path(args.output_dir)

    all_data = read_or_rows(str(base / "all_authors_or_table_csv" / "*.csv"))
    top_data = read_or_rows(str(base / "top_cited_or_table_csv" / "*.csv"))
    pub_label = infer_pub_label(all_data)

    if "paper-year" in pub_label.lower():
        pub_text = "Publication volume is modelled as a continuous variable (OR per additional active publication year)."
    else:
        pub_text = "Publication volume is modelled as a continuous variable (OR per additional paper)."

    suffix = f"_{args.suffix}" if args.suffix else ""

    all_caption = (
        "Table XXX. Univariable and multivariable odds ratios (OR) and 95% confidence intervals (CI) "
        "from logistic regression models estimating the probability of having >=1 retraction, among all authors. "
        f"{pub_text} Reference categories: Women; <1992; High income level; No top-cited status; Clinical Medicine."
    )
    top_caption = (
        "Table XXX. Univariable and multivariable odds ratios (OR) and 95% confidence intervals (CI) "
        "from logistic regression models estimating the probability of having >=1 retraction, among top-cited authors. "
        f"{pub_text} Reference categories: Women; <1992; High income level; Clinical Medicine."
    )

    build_doc(
        template,
        out_dir / f"Table_all_authors_OR{suffix}.docx",
        all_data,
        include_top_cited_row=True,
        pub_label=pub_label,
        caption=all_caption,
    )
    build_doc(
        template,
        out_dir / f"Table_top_cited_authors_OR{suffix}.docx",
        top_data,
        include_top_cited_row=False,
        pub_label=pub_label,
        caption=top_caption,
    )

    print("Generated:", out_dir / f"Table_all_authors_OR{suffix}.docx")
    print("Generated:", out_dir / f"Table_top_cited_authors_OR{suffix}.docx")


if __name__ == "__main__":
    main()
