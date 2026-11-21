"""Refresh the local pass counts in this release's contract matrix."""
import argparse
from pathlib import Path
import re

HERE = Path(__file__).resolve().parent
INVENTORY = HERE / "features-and-cases.md"
CONTRACTS = HERE / "named-contracts.md"
STATES = {"PASS", "FAIL", "SKIP", "UNRUN"}
CHANNELS = ("Standard", "Experimental")
SUMMARY_START = "Across recorded local runs, "


def registered_results():
    feature_rows, case_rows = INVENTORY.read_text().split("## Complete registered test inventory")
    results = {}
    for row in case_rows.splitlines():
        if row.startswith("| [") and "/conformance/tests/" in row:
            cells = [part.strip() for part in row.split("|")]
            name = re.match(r"\[([^\]]+)\]", cells[1]).group(1)
            passes = []
            for cell in cells[4:6]:
                states = [outcome.split(" ", 1)[0] for outcome in cell.split(" · ")]
                if any(state not in STATES for state in states) or ("UNRUN" in states and len(states) > 1):
                    raise ValueError(f"Invalid case outcome for {name}: {cell}")
                passes.append("PASS" in states)
            if name in results or len(passes) != len(CHANNELS):
                raise ValueError(f"Invalid case entry for {name}")
            results[name] = passes

    by_feature = {}
    for row in feature_rows.splitlines():
        if row.startswith("| ["):
            cells = row.split("|")
            name = re.match(r"\[([^\]]+)\]", cells[1].strip()).group(1)
            cases = re.findall(r"\[([^\]]+)\]\([^)]*/conformance/tests/[^)]+\)", cells[-2])
            if name in by_feature or len(cases) != len(set(cases)):
                raise ValueError(f"Invalid feature entry for {name}")
            by_feature[name] = [results[case] for case in cases]
    if len(by_feature) != 64 or len(results) != 170:
        raise ValueError("Expected the v1.7.0 registry of 64 features and 170 cases")
    return by_feature, results


def render(text, by_feature, results):
    lines = text.splitlines()
    summary = next(i for i, line in enumerate(lines) if line.startswith(SUMMARY_START))
    standard, experimental = [sum(column) for column in zip(*results.values())]
    all_passed = sum(bool(cases) and all(all(outcomes) for outcomes in cases) for cases in by_feature.values())
    lines[summary] = (
        f"{SUMMARY_START}**{standard} of {len(results)}** registered cases have a PASS on Standard and "
        f"**{experimental} of {len(results)}** have a PASS on Experimental. "
        f"**{all_passed} of {len(by_feature)}** named features have at least one registered case and "
        "a PASS for every associated case on both channels."
    )
    header = next(i for i, line in enumerate(lines) if line.startswith("| Feature | Contract and support |"))
    seen = set()
    for i in range(header + 2, len(lines)):
        if not lines[i].startswith("| `"):
            break
        cells = lines[i].split("|")
        feature = cells[1].strip().strip("`")
        if feature in seen:
            raise ValueError(f"Duplicate named contract for {feature}")
        cases = by_feature[feature]
        summaries = [f"{sum(column)} / {len(cases)}" for column in zip(*cases)]
        if not cases:
            summaries = ["No registered case"] * len(CHANNELS)
        lines[i] = "|".join(cells[:6]) + "| " + " | ".join(summaries) + " |"
        seen.add(feature)
    if seen != by_feature.keys():
        raise ValueError("The named contract table differs from the upstream feature registry")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    original = CONTRACTS.read_text()
    updated = render(original, *registered_results())
    if args.check:
        if updated != original:
            raise SystemExit("Per-feature counts have changed. Run feature-status.py to update the matrix.")
    elif updated != original:
        CONTRACTS.write_text(updated)
