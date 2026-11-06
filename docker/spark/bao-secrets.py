#!/usr/bin/env python3
"""Lit dans OpenBao les secrets de la couche Silver et les affiche sous forme de commandes « export ».

Utilisé par le lanceur `silver` :   secrets="$(python3 /usr/local/bin/bao-secrets.py)"; eval "$secrets"

- Si BAO_ADDR n'est pas défini, ne fait rien : on retombe sur les variables d'environnement (tests locaux, `make demo`).
- Si BAO_ADDR est défini, TOUT échec (coffre injoignable, jeton refusé, secret absent) arrête le lancement :
  on ne démarre jamais Silver avec une clé de pseudonymisation absente ou approximative.
- Rien n'est écrit dans les journaux à part les NOMS des variables (jamais les valeurs).
Aucune dépendance : bibliothèque standard seulement.
"""
import json
import os
import shlex
import sys
import urllib.error
import urllib.request

# chemin dans le coffre -> {champ du secret: [variables d'environnement à définir]}
SECRETS = {
    "secret/data/chu/pseudonymisation": {"cle": ["SILVER_HMAC_KEY"]},
    "secret/data/chu/s3": {"access_key": ["S3_ACCESS_KEY"], "secret_key": ["S3_SECRET_KEY"]},
    "secret/data/chu/postgres_etl": {"user": ["POSTGRES_USER", "PG_USER"], "password": ["POSTGRES_PASSWORD", "PG_PASSWORD"]},
}


def lire(addr: str, token: str, chemin: str) -> dict:
    req = urllib.request.Request(f"{addr.rstrip('/')}/v1/{chemin}", headers={"X-Vault-Token": token})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)["data"]["data"]


def main() -> int:
    addr = os.getenv("BAO_ADDR")
    if not addr:
        return 0
    token_file = os.getenv("BAO_TOKEN_FILE", "/run/bao/silver.token")
    try:
        token = os.getenv("BAO_TOKEN") or open(token_file, encoding="utf-8").read().strip()
    except OSError as exc:
        print(f"[silver] jeton OpenBao illisible ({token_file}) : {exc}", file=sys.stderr)
        return 1
    noms = []
    for chemin, champs in SECRETS.items():
        try:
            data = lire(addr, token, chemin)
        except urllib.error.HTTPError as exc:
            print(f"[silver] OpenBao a refusé la lecture de {chemin} (HTTP {exc.code})", file=sys.stderr)
            return 1
        except Exception as exc:  # coffre injoignable, délai dépassé, réponse invalide
            print(f"[silver] OpenBao injoignable pour {chemin} : {exc}", file=sys.stderr)
            return 1
        for champ, variables in champs.items():
            if not data.get(champ):
                print(f"[silver] champ « {champ} » absent de {chemin}", file=sys.stderr)
                return 1
            for var in variables:
                print(f"export {var}={shlex.quote(str(data[champ]))}")
                noms.append(var)
    print(f"[silver] secrets lus dans OpenBao : {', '.join(noms)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
