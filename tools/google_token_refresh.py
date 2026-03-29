"""Refresh Google OAuth access token and store it in OpenBao.

Reads OAuth credentials (client_id, client_secret, refresh_token) from
secret/blobapi/google, exchanges for a fresh access_token, and writes
the token to secret/blobapi/google_token for blobhttp vault integration.

Run periodically (e.g., every 45 minutes) or once at session start.
Google access tokens expire after 1 hour; blobhttp caches vault secrets
for 5 minutes, so the token in vault must be refreshed before expiry.

Usage:
    uv run python tools/google_token_refresh.py
    uv run python tools/google_token_refresh.py --vault-addr http://127.0.0.1:8200
"""

import argparse
import sys
import time

import requests


def refresh_and_store(vault_addr, vault_token):
    """Fetch OAuth creds from vault, exchange for access token, store back."""
    # Read OAuth credentials
    r = requests.get(
        f"{vault_addr}/v1/secret/data/blobapi/google",
        headers={"X-Vault-Token": vault_token},
    )
    r.raise_for_status()
    creds = r.json()["data"]["data"]

    # Exchange refresh token for access token
    t = requests.post(creds["token_uri"], data={
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
        "refresh_token": creds["refresh_token"],
        "grant_type": "refresh_token",
    })
    t.raise_for_status()
    token_data = t.json()

    access_token = token_data["access_token"]
    expires_in = token_data.get("expires_in", 3600)
    expires_at = int(time.time()) + expires_in

    # Write access token to a dedicated vault path
    w = requests.post(
        f"{vault_addr}/v1/secret/data/blobapi/google_token",
        headers={"X-Vault-Token": vault_token},
        json={"data": {
            "access_token": access_token,
            "expires_at": expires_at,
            "quota_project": creds.get("quota_project", "meplex-integration"),
        }},
    )
    w.raise_for_status()
    version = w.json()["data"]["version"]

    print(f"access_token: {access_token[:20]}... (expires in {expires_in}s)")
    print(f"stored at secret/blobapi/google_token (version {version})")
    return access_token, expires_at


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vault-addr", default="http://127.0.0.1:8200")
    parser.add_argument("--vault-token", default="dev-blobapi-token")
    args = parser.parse_args()

    refresh_and_store(args.vault_addr, args.vault_token)


if __name__ == "__main__":
    main()
