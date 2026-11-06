# OpenBao : coffre de secrets du projet

Le Livrable 1 prévoit OpenBao pour « les mots de passe des bases, l'accès FTP, la clé de pseudonymisation ;
aucun secret dans Git ». Cette version est le **niveau 1 : coffre en mode développement**.

## Ce qui est en place

| Élément | Rôle |
|---|---|
| service `openbao` | le coffre (http://localhost:8200), mode développement, journal d'audit actif |
| service `openbao-init` | à chaque `docker compose up`, range les secrets du `.env` dans le coffre, crée la politique `silver` et un jeton de lecture, puis s'arrête |
| `docker/spark/bao-secrets.py` | lit dans le coffre les secrets de Silver et les passe au lanceur `silver` |
| `docker/spark/silver` | appelle `bao-secrets.py` avant `spark-submit` ; **s'arrête** si la lecture échoue |
| `docker/openbao/audit.hcl` | déclare le journal d'audit (sortie standard du conteneur `openbao`) |

## Secrets rangés (`secret/chu/...`)

| Chemin | Champs | Lu par Silver ? |
|---|---|:-:|
| `pseudonymisation` | `cle` (clé HMAC-SHA256) | oui |
| `s3` | `access_key`, `secret_key` | oui |
| `postgres_etl` | `user`, `password` | oui |
| `postgres_bi` | `user`, `password` | non |
| `postgres_admin` | `user`, `password` | non |
| `sftp` | `user`, `password` (seulement si `SFTP_USER` et `SFTP_PASSWORD` sont dans le `.env` racine) | non |

Le jeton de Silver (politique `silver`) ne peut **lire que** `pseudonymisation`, `s3` et `postgres_etl` : lecture
de `postgres_admin` ou `postgres_bi` et toute écriture sont refusées (HTTP 403).

## Commandes utiles

```bash
# voir les secrets rangés (jeton racine = OPENBAO_ROOT_TOKEN du .env)
docker compose exec -e BAO_ADDR=http://127.0.0.1:8200 -e BAO_TOKEN=<OPENBAO_ROOT_TOKEN> openbao bao kv list secret/chu

# lire la clé de pseudonymisation
docker compose exec -e BAO_ADDR=http://127.0.0.1:8200 -e BAO_TOKEN=<OPENBAO_ROOT_TOKEN> openbao bao kv get -field=cle secret/chu/pseudonymisation

# qui a lu quoi (journal d'audit : les valeurs y sont hachées, jamais en clair)
docker compose logs openbao | grep '"type":"request"' | grep secret/data/chu

# lancer un job Silver (inchangé) : la clé vient du coffre
docker compose exec spark silver run --job all
```

## Limites assumées (à dire dans le rapport)

- **Mode développement = coffre en mémoire.** Si le conteneur `openbao` redémarre seul, il est vidé :
  `docker compose up -d --force-recreate openbao-init` le remplit de nouveau. Le `.env` (non versionné) reste la
  **graine** des secrets ; ce qu'apporte le coffre, c'est que les services les **lisent au moment d'en avoir besoin**,
  avec un jeton à droits limités et un journal d'audit, au lieu de les recevoir dans leur environnement.
- **Les conteneurs PostgreSQL, pgAdmin et Superset reçoivent encore leurs mots de passe par leur environnement** :
  Docker les leur donne au démarrage, avant qu'ils puissent interroger un coffre.
- Pas de TLS entre conteneurs, jeton racine dans le `.env`, un seul nœud, pas de déverrouillage (« unseal »).
- **En production** : OpenBao en haute disponibilité chez l'hébergeur certifié HDS, TLS partout, initialisation et
  déverrouillage par plusieurs détenteurs de clés, authentification par AppRole ou Kubernetes (un compte par service),
  rotation de la clé de pseudonymisation. **Ne jamais changer la clé HMAC** une fois les données pseudonymisées :
  tous les pseudonymes changeraient et il faudrait tout recharger.

## Pour aller plus loin (niveau 2)

Un compte AppRole par outil (Airflow, Superset…) avec sa propre politique, et la lecture des secrets par
Airflow (connexions) : environ une demi-journée, à faire en même temps qu'Airflow.
