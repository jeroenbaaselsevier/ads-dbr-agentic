# Lessons Learned: Journal Author Ranking Session (2026-03-18)

## Scope
This note captures session-specific lessons from the Stanford top-cited journal-ranking workstream.
It is intended as input for future agent-knowledge updates, not a direct knowledge-base edit.

## Session Context
- Project area: journal-level indicators derived from top-cited author rankings.
- Core outputs produced in this session:
  - baseline scenario/sensitivity analyses
  - 3-year rolling-window trend analyses
  - comparisons with Source metrics (SNIP, SJR, CiteScore)
  - diagnostics on first-publication-year completeness and career-age hypotheses

## Confirmed Technical Learnings

### 1) Correct ADS table for author first publication year
- Use `snapshot_functions.ads.author.get_table('Author_Info_and_H_Index')`.
- Use column `First_year_in_scopus` (case-sensitive in this table snapshot).
- This was explicitly confirmed and should be preferred for first-year diagnostics over heuristic table selection.

Suggested knowledge update:
- Add a specific reference note for first-year diagnostics pointing to `Author_Info_and_H_Index.First_year_in_scopus`.

### 2) Sliding-window alignment for Source metrics
- For rolling windows, Source metrics should be aligned to the window end year.
- SNIP/SJR come from `source.metrics` array; CiteScore comes from `source.calculations[].csMetric.csCiteScore`.
- Building a single per-source/year metric table first and reusing it across windows simplified joins and reduced errors.

Suggested knowledge update:
- Add a recipe snippet for: rolling-window indicator + window-end Source metric alignment.

### 3) CSV robustness issue in local post-processing
- Some exported CSV rows produced parser issues (`on_bad_lines='skip'` needed locally).
- Likely due rare title-string quoting edge cases in very large source-title fields.
- Local analysis remained possible, but with tolerant parsing warnings.

Suggested knowledge update:
- For large text-heavy outputs, export parquet alongside CSV for lossless local analysis.
- If CSV is required, document tolerant parsing and row-count validation checks.

### 4) Lightweight run polling works, but script quoting is fragile
- Frequent one-line polling (`jobs get-run`) is effective and low-overhead.
- Inline Python quoting inside bash loops is easy to break.

Suggested knowledge update:
- Add a known-good polling snippet in runbook form with robust quoting.
- Prefer minimal one-shot status commands during active troubleshooting.

### 5) Interactive visuals with markdown
- Plotly interactivity works well via standalone `.html` outputs linked from markdown.
- Direct embedded interactivity in `.md` depends on renderer support; links are reliable.

Suggested knowledge update:
- Add reporting guidance: static PNG for portability + linked Plotly HTML for exploration.

## Analytic Learnings (Interpretation)

### 6) Upward score trend is not explained by ADS first-year missingness spike
- Using `Author_Info_and_H_Index.First_year_in_scopus`:
  - ADS first-year null rate was low and only slightly increasing across windows.
  - Stanford first-year null rate was higher, but ADS did not show a large recent-year null shock.
- Therefore, missing ADS first-year coverage does not appear to be the primary cause of score drift.

Suggested knowledge update:
- Add a diagnostic checklist item: test null-rate trajectory before attributing trend drift to missingness.

### 7) Career-age hypothesis test outcome
- Top-15 career age trend (ADS and Stanford variants) increased over windows in this run.
- This did not support the specific hypothesis that selected top-15 became systematically younger over time.

Suggested knowledge update:
- Add a standard trend-diagnosis block:
  - panel composition effect
  - balanced-panel trend
  - first-year null-rate trend
  - career-age trend

### 8) Composition effects are material
- New-vs-continuing journal mix meaningfully shifted aggregate distributions.
- Balanced-panel checks are necessary for trend interpretation.

Suggested knowledge update:
- Add a default requirement for balanced-panel sensitivity in rolling-window analyses.

## Communication Learnings

### 9) Avoid normative framing for metric direction
- Phrases like "best/better" created confusion because the metric is a construct-specific signal.
- Neutral framing improved interpretability and scientific tone.

Suggested knowledge update:
- Add guidance language:
  - "lower/higher on this metric" instead of normative quality claims
  - explicitly separate indicator construct from intrinsic journal quality judgments

## Candidate Additions to Agent Knowledge (Summary)
1. Reference-level note: author first-year field = `Author_Info_and_H_Index.First_year_in_scopus`.
2. Recipe: rolling-window journal indicator + Source metric year alignment.
3. Runbook: robust lightweight polling snippet.
4. Reporting standard: export parquet companion for large CSVs.
5. Trend diagnostics checklist: composition, balanced panel, null-rate trajectory, career age.
6. Wording standard: non-normative interpretation of rank-based indicators.

## Session Artifact Linkage
This lesson note belongs to the current session thread and is intentionally session-scoped to avoid collisions with other projects.
