"""Refresh the per-feature counts in this release's contract matrix."""
import argparse
from collections import Counter
from pathlib import Path
import re

HERE = Path(__file__).resolve().parent
INVENTORY = HERE / "features-and-cases.md"
CONTRACTS = HERE / "named-contracts.md"
STATES = ("PASS", "FAIL", "SKIP", "UNRUN")
COLUMNS = ("Standard cases P/F/S/U", "Experimental cases P/F/S/U")


def registered_results():
    feature_rows, case_rows = INVENTORY.read_text().split("## Complete registered test inventory")
    results = {}
    for row in case_rows.splitlines():
        if row.startswith("| [") and "/conformance/tests/" in row:
            cells = [part.strip() for part in row.split("|")]
            name = re.match(r"\[([^\]]+)\]", cells[1]).group(1)
            status = tuple(cell.split()[0] for cell in cells[4:6])
            if name in results or any(value not in STATES for value in status):
                raise ValueError(f"Invalid case entry for {name}")
            results[name] = status
    by_feature = {}
    for row in feature_rows.splitlines():
        if row.startswith("| ["):
            cells = row.split("|")
            name = re.match(r"\[([^\]]+)\]", cells[1].strip()).group(1)
            cases = re.findall(r"\[([^\]]+)\]\([^)]*/conformance/tests/[^)]+\)", cells[-2])
            by_feature[name] = [results[case] for case in cases]
    if len(by_feature) != 64 or len(results) != 170:
        raise ValueError("Expected the v1.7.0 registry of 64 features and 170 cases")
    return by_feature


def render(text, results):
    lines = text.splitlines()
    header = next(i for i, line in enumerate(lines) if line.startswith("| Feature | Contract and support |"))
    original_columns = lines[header].removesuffix(" " + " | ".join(COLUMNS) + " |")
    lines[header] = original_columns.rstrip(" |") + " | " + " | ".join(COLUMNS) + " |"
    lines[header + 1] = "|---|---|---|---|---|---|---|"
    seen = set()
    for i in range(header + 2, len(lines)):
        if not lines[i].startswith("| `"):
            break
        cells = lines[i].split("|")
        feature = cells[1].strip().strip("`")
        cases = results[feature]
        counts = [Counter(outcome[column] for outcome in cases) for column in (0, 1)]
        summaries = [" / ".join(str(count[state]) for state in STATES) if cases else "No registered case" for count in counts]
        lines[i] = "|".join(cells[:6]) + "| " + " | ".join(summaries) + " |"
        seen.add(feature)
    if seen != results.keys():
        raise ValueError("The named contract table differs from the upstream feature registry")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    original = CONTRACTS.read_text()
    updated = render(original, registered_results())
    if args.check:
        if updated != original:
            raise SystemExit("Per-feature counts have changed. Run feature-status.py to update the matrix.")
    elif updated != original:
        CONTRACTS.write_text(updated)
