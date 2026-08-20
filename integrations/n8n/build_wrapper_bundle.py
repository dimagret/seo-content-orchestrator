"""Build or verify the deterministic local-only Stage B n8n bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from integrations.n8n.wrapper import build_stage_b_bundle, validate_stage_b_bundle

_DIRECTORY = Path(__file__).parent
_DEFAULT_SOURCE = _DIRECTORY / "source-workflow.json"
_DEFAULT_CONTRACT = _DIRECTORY / "universal-contract.json"
_DEFAULT_OUTPUT = _DIRECTORY / "stage-b-local-bundle.json"


def _load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise TypeError(f"{path}: JSON object required")
    return value


def render_bundle_json(source: Path, contract: Path) -> bytes:
    """Render deterministic, newline-terminated JSON after static validation."""

    bundle = build_stage_b_bundle(_load_object(source), _load_object(contract))
    report = validate_stage_b_bundle(bundle)
    if not report.is_valid:
        raise ValueError("generated bundle failed static validation")
    return (
        json.dumps(bundle, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=_DEFAULT_SOURCE)
    parser.add_argument("--contract", type=Path, default=_DEFAULT_CONTRACT)
    parser.add_argument("--output", type=Path, default=_DEFAULT_OUTPUT)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail unless output already equals the deterministic render",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    rendered = render_bundle_json(args.source, args.contract)
    digest = hashlib.sha256(rendered).hexdigest()
    if args.check:
        if not args.output.is_file() or args.output.read_bytes() != rendered:
            print("stage-b local bundle check failed")
            return 1
        print(f"stage-b local bundle check passed sha256={digest}")
        return 0
    args.output.write_bytes(rendered)
    print(f"wrote {args.output} sha256={digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
