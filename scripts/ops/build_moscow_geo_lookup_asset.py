from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.discovery.geo_lookup import GEO_LOOKUP_SOURCE_CONTRACT, write_geo_lookup_asset


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a compact runtime Moscow geo lookup asset from the operator settlement export."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the operator CSV exported by build_moscow_geo_zone.py.",
    )
    parser.add_argument(
        "--output",
        default=str(REPO_ROOT / "app" / "discovery" / "data" / "moscow_geo_lookup.json"),
        help="Path to the compact JSON lookup asset.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    args = parse_args(argv)
    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    if not input_path.is_file():
        raise SystemExit(f"Input CSV does not exist: {input_path}")

    with input_path.open("r", encoding="utf-8-sig", newline="") as source_file:
        reader = csv.DictReader(source_file)
        payload = write_geo_lookup_asset(
            output_path,
            reader,
            source_contract=GEO_LOOKUP_SOURCE_CONTRACT,
        )

    build_stats = payload.get("build_stats") or {}
    print(f"OUTPUT {output_path}")
    print(f"RECORDS {payload['record_count']}")
    print(f"AMBIGUOUS_RECORDS {payload.get('ambiguous_record_count', 0)}")
    print(f"AUTO_MERGED_CLUSTERS {build_stats.get('auto_merged_cluster_count', 0)}")
    print(f"AMBIGUOUS_CLUSTERS {build_stats.get('ambiguous_cluster_count', 0)}")
    print(f"OVERRIDE_ENTRIES {build_stats.get('override_entry_count', 0)}")
    print(f"SCHEMA {payload['schema_version']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
