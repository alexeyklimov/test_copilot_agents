#!/usr/bin/env python3
import argparse
import json
import pathlib
import sys


def load_results(path):
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    results = {}
    for entry in payload:
        params = tuple(sorted((entry.get("params") or {}).items()))
        key = (entry["benchmark"], entry["mode"], params)
        results[key] = {
            "score": float(entry["primaryMetric"]["score"]),
            "unit": entry["primaryMetric"]["scoreUnit"],
        }
    return results


def format_params(params):
    if not params:
        return "-"
    return ", ".join(f"{name}={value}" for name, value in params)


def regression_ratio(mode, base_score, candidate_score):
    if base_score == 0:
        return 0.0
    if mode == "thrpt":
        return (base_score - candidate_score) / base_score
    return (candidate_score - base_score) / base_score


def main():
    parser = argparse.ArgumentParser(description="Compare two JMH result files.")
    parser.add_argument("--base", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--threshold", type=float, default=0.15)
    parser.add_argument("--summary", required=False)
    args = parser.parse_args()

    base_results = load_results(args.base)
    candidate_results = load_results(args.candidate)

    missing = sorted(set(base_results) - set(candidate_results))
    candidate_only = sorted(set(candidate_results) - set(base_results))
    regressions = []
    rows = []

    for key in sorted(set(base_results) & set(candidate_results)):
        benchmark, mode, params = key
        base_metric = base_results[key]
        candidate_metric = candidate_results[key]
        ratio = regression_ratio(mode, base_metric["score"], candidate_metric["score"])
        rows.append(
            "| `{}` | `{}` | {} | {:.3f} | {:.3f} | {:.2%} | {} |".format(
                benchmark,
                mode,
                format_params(params),
                base_metric["score"],
                candidate_metric["score"],
                ratio,
                base_metric["unit"],
            )
        )
        if ratio > args.threshold:
            regressions.append((key, ratio))

    lines = [
        "# JMH regression summary",
        "",
        f"- threshold: {args.threshold:.0%}",
        f"- compared benchmarks: {len(rows)}",
        f"- regressions: {len(regressions)}",
        f"- missing entries: {len(missing)}",
        f"- candidate-only entries: {len(candidate_only)}",
        "",
        "| Benchmark | Mode | Params | Base | Candidate | Regression | Unit |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
        *rows,
    ]

    if missing:
        lines.extend(
            [
                "",
                "## Missing benchmark entries",
                "",
                *[
                    f"- `{benchmark}` / `{mode}` / {format_params(params)}"
                    for benchmark, mode, params in missing
                ],
            ]
        )

    if candidate_only:
        lines.extend(
            [
                "",
                "## Candidate-only benchmark entries",
                "",
                *[
                    f"- `{benchmark}` / `{mode}` / {format_params(params)}"
                    for benchmark, mode, params in candidate_only
                ],
            ]
        )

    summary = "\n".join(lines) + "\n"
    print(summary)

    if args.summary:
        summary_path = pathlib.Path(args.summary)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(summary, encoding="utf-8")

    if regressions or missing:
        sys.exit(1)


if __name__ == "__main__":
    main()
