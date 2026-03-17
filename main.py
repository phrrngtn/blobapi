"""blobapi — OpenAPI spec catalog and LLM pricing reference data.

Usage:
    main.py [-c CONNECTION] init
    main.py [-c CONNECTION] sync [--repo DIR] [--full]
    main.py [-c CONNECTION] catalog [--repo DIR]
    main.py [-c CONNECTION] adapters [--dir DIR]
    main.py [-c CONNECTION] bootstrap-pricing [--providers FILE]
                            [--bifrost-url URL] [--container NAME]
                            [--image-created TIMESTAMP]
    main.py [-c CONNECTION] scrape-pricing [PROVIDER...]
                            [--model MODEL] [--bifrost-chat URL]
    main.py connections
    main.py -h | --help

Commands:
    init                Create database tables.
    sync                HEAD snapshot → full shred (use --full for git history TTST).
    catalog             Fast metadata-only catalog (no YAML parse, < 2s).
    adapters            Load adapter YAML files into DB.
    bootstrap-pricing   Seed provider refs + pricing from Bifrost.
    scrape-pricing      Scrape pricing pages via Jina Reader + LLM extraction.
    connections         List available connections from connections.toml.

Options:
    -c CONNECTION --connection CONNECTION  Named connection [default: default].
    --repo DIR          Path to cloned openapi-directory repo [default: openapi-directory].
    --full              Walk full git history for complete TTST (slower).
    --dir DIR           Path to adapters directory [default: adapters].
    --providers FILE    Path to providers.yaml [default: adapters/providers.yaml].
    --bifrost-url URL   Bifrost /v1/models endpoint [default: http://localhost:8080/v1/models].
    --bifrost-chat URL  Bifrost chat completions endpoint [default: http://localhost:8080/v1/chat/completions].
    --container NAME    Docker container name for image date [default: bifrost].
    --image-created TIMESTAMP  Override sys_from (ISO 8601).
    --model MODEL       LLM model for extraction [default: anthropic/claude-haiku-4-5-20251001].
    -h --help           Show this help.
"""

import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from docopt import docopt
from sqlalchemy.orm import Session

from blobapi.config import create_engine, list_connections, load_config
from blobapi.models import Base


def _engine(connection_name: str):
    return create_engine(connection_name)


def cmd_init(opts):
    engine = _engine(opts["--connection"])
    Base.metadata.create_all(engine)
    print(f"Tables created on [{opts['--connection']}]")


def cmd_sync(opts):
    from blobapi.git_scraper import sync

    engine = _engine(opts["--connection"])
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        result = sync(
            session,
            clone_dir=Path(opts["--repo"]),
            full_history=opts["--full"],
        )
    print(
        f"{result['specs']} specs, {result['paths']} paths, "
        f"{result['operations']} operations in {result['elapsed']}"
    )


def cmd_catalog(opts):
    from blobapi.git_scraper import catalog

    engine = _engine(opts["--connection"])
    Base.metadata.create_all(engine)
    t0 = time.monotonic()
    with Session(engine) as session:
        result = catalog(session, clone_dir=Path(opts["--repo"]))
    elapsed = time.monotonic() - t0
    print(f"{result['specs']} specs cataloged in {elapsed:.2f}s (commit {result['commit_sha'][:8]})")


def cmd_adapters(opts):
    import yaml
    from sqlalchemy import select
    from blobapi.models import ApiAdapter

    try:
        from yaml import CSafeLoader as SafeLoader
    except ImportError:
        from yaml import SafeLoader

    engine = _engine(opts["--connection"])
    Base.metadata.create_all(engine)

    adapter_dir = Path(opts["--dir"])
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
                # Skip LLM adapter entries (have prompt_template, not call_jmespath)
                if "prompt_template" in entry or "call_jmespath" not in entry:
                    continue

                provider = entry["provider"]
                api_name = entry["api_name"]
                path = entry["path"]
                method = entry["method"]

                existing = session.execute(
                    select(ApiAdapter).filter_by(
                        provider=provider, api_name=api_name,
                        path=path, method=method, sys_to=None,
                    )
                ).scalar_one_or_none()

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


def cmd_bootstrap_pricing(opts):
    from blobapi.bootstrap_pricing import bootstrap_prices, load_providers

    engine = _engine(opts["--connection"])
    Base.metadata.create_all(engine)

    now = datetime.now(timezone.utc)
    image_created = None
    if opts["--image-created"]:
        image_created = datetime.fromisoformat(opts["--image-created"])

    with Session(engine) as session:
        providers_path = Path(opts["--providers"])
        if providers_path.exists():
            n = load_providers(session, providers_path, now)
            print(f"Providers: {n} changed")

        result = bootstrap_prices(
            session,
            bifrost_url=opts["--bifrost-url"],
            image_created=image_created,
            container_name=opts["--container"],
        )
        print(
            f"Pricing: {result['inserted']} inserted, "
            f"{result['closed']} closed, {result['skipped']} unchanged"
        )

        session.commit()


def cmd_scrape_pricing(opts):
    from blobapi.scrape_pricing import scrape_all

    engine = _engine(opts["--connection"])
    Base.metadata.create_all(engine)

    providers = opts["PROVIDER"] or None  # docopt returns [] if none given

    with Session(engine) as session:
        result = scrape_all(
            session,
            providers=providers or None,
            bifrost_url=opts["--bifrost-chat"],
            model=opts["--model"],
        )
        print(
            f"Scrape: {result['inserted']} inserted, "
            f"{result['closed']} closed, {result['skipped']} unchanged, "
            f"{result['errors']} errors"
        )
        session.commit()


def cmd_connections(opts):
    config = load_config()
    for name in list_connections(config):
        dialect = config[name].get("dialect", "?")
        print(f"  {name:20s}  ({dialect})")


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    opts = docopt(__doc__)

    if opts["init"]:
        cmd_init(opts)
    elif opts["sync"]:
        cmd_sync(opts)
    elif opts["catalog"]:
        cmd_catalog(opts)
    elif opts["adapters"]:
        cmd_adapters(opts)
    elif opts["bootstrap-pricing"]:
        cmd_bootstrap_pricing(opts)
    elif opts["scrape-pricing"]:
        cmd_scrape_pricing(opts)
    elif opts["connections"]:
        cmd_connections(opts)
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
