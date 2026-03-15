"""
CLI entry point for blobapi.

Usage:
    uv run python main.py init                              # Create tables
    uv run python main.py sync                              # HEAD snapshot → full shred (< 30s)
    uv run python main.py sync --full                       # Full git history → TTST reconstruction
    uv run python main.py catalog                           # Fast metadata-only (< 2s, no YAML parse)
    uv run python main.py adapters                          # Load adapter YAML files into DB
    uv run python main.py connections                       # List available connections
"""

import argparse
import logging
import sys
import time
from pathlib import Path

from sqlalchemy.orm import Session

from blobapi.config import create_engine, list_connections, load_config
from blobapi.models import Base


def _engine(connection_name: str):
    return create_engine(connection_name)


def cmd_init(args):
    engine = _engine(args.connection)
    Base.metadata.create_all(engine)
    print(f"Tables created on [{args.connection}]")


def cmd_sync(args):
    from blobapi.git_scraper import sync

    engine = _engine(args.connection)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        result = sync(
            session,
            clone_dir=Path(args.repo),
            full_history=args.full,
        )
    print(
        f"{result['specs']} specs, {result['paths']} paths, "
        f"{result['operations']} operations in {result['elapsed']}"
    )


def cmd_catalog(args):
    from blobapi.git_scraper import catalog

    engine = _engine(args.connection)
    Base.metadata.create_all(engine)
    t0 = time.monotonic()
    with Session(engine) as session:
        result = catalog(session, clone_dir=Path(args.repo))
    elapsed = time.monotonic() - t0
    print(f"{result['specs']} specs cataloged in {elapsed:.2f}s (commit {result['commit_sha'][:8]})")


def cmd_adapters(args):
    import yaml
    from datetime import datetime, timezone
    from sqlalchemy import select
    from blobapi.models import ApiAdapter

    try:
        from yaml import CSafeLoader as SafeLoader
    except ImportError:
        from yaml import SafeLoader

    engine = _engine(args.connection)
    Base.metadata.create_all(engine)

    adapter_dir = Path(args.dir)
    if not adapter_dir.exists():
        print(f"No adapters directory at {adapter_dir}", file=sys.stderr)
        sys.exit(1)

    now = datetime.now(timezone.utc)
    loaded = 0

    with Session(engine) as session:
        for yaml_path in sorted(adapter_dir.glob("*.yaml")):
            with open(yaml_path) as f:
                entries = yaml.load(f, Loader=SafeLoader)

            if not isinstance(entries, list):
                continue

            for entry in entries:
                provider = entry["provider"]
                api_name = entry["api_name"]
                path = entry["path"]
                method = entry["method"]

                # Close any existing current version
                existing = session.execute(
                    select(ApiAdapter).filter_by(
                        provider=provider, api_name=api_name,
                        path=path, method=method, sys_to=None,
                    )
                ).scalar_one_or_none()

                # Skip if unchanged
                if existing and (
                    existing.call_jmespath == entry["call_jmespath"]
                    and existing.response_jmespath == entry["response_jmespath"]
                    and existing.base_url == entry.get("base_url")
                    and existing.vault_secret == entry.get("vault_secret")
                ):
                    continue

                if existing:
                    existing.sys_to = now

                session.add(ApiAdapter(
                    provider=provider,
                    api_name=api_name,
                    path=path,
                    method=method,
                    base_url=entry.get("base_url"),
                    vault_secret=entry.get("vault_secret"),
                    call_jmespath=entry["call_jmespath"],
                    call_notes=entry.get("call_notes"),
                    response_jmespath=entry["response_jmespath"],
                    response_notes=entry.get("response_notes"),
                    sys_from=now,
                ))
                loaded += 1

        session.commit()

    print(f"Loaded {loaded} adapters from {adapter_dir}")


def cmd_connections(args):
    config = load_config()
    for name in list_connections(config):
        dialect = config[name].get("dialect", "?")
        print(f"  {name:20s}  ({dialect})")


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="blobapi — OpenAPI spec catalog")
    parser.add_argument(
        "-c", "--connection",
        default="default",
        help="Named connection from connections.toml (default: 'default')",
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("init", help="Create database tables")

    sp_sync = sub.add_parser("sync", help="Sync from git: YAML→JSON→TTST tables")
    sp_sync.add_argument("--repo", default="openapi-directory", help="Path to cloned repo")
    sp_sync.add_argument("--full", action="store_true",
                        help="Walk full git history for complete TTST (slower)")

    sp_catalog = sub.add_parser("catalog", help="Fast metadata-only catalog (no YAML parse)")
    sp_catalog.add_argument("--repo", default="openapi-directory", help="Path to cloned repo")

    sp_adapters = sub.add_parser("adapters", help="Load adapter YAML files into DB")
    sp_adapters.add_argument("--dir", default="adapters", help="Path to adapters directory")

    sub.add_parser("connections", help="List available connections")

    args = parser.parse_args()
    commands = {
        "init": cmd_init,
        "sync": cmd_sync,
        "catalog": cmd_catalog,
        "adapters": cmd_adapters,
        "connections": cmd_connections,
    }
    handler = commands.get(args.command)
    if handler:
        handler(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
