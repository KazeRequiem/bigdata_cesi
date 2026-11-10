# Configuration Superset du projet (chargée via SUPERSET_CONFIG_PATH)
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Base de métadonnées de Superset (utilisateurs, graphiques, tableaux de bord)
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{u}:{p}@postgres:5432/superset".format(
    u=os.environ["POSTGRES_USER"], p=os.environ["POSTGRES_PASSWORD"]
)

BABEL_DEFAULT_LOCALE = "fr"
LANGUAGES = {
    "fr": {"flag": "fr", "name": "Français"},
    "en": {"flag": "us", "name": "English"},
}

# --- Livrable 3 : tableaux de bord (docker/superset/dashboards/construire_dashboards.py) ---

FEATURE_FLAGS = {
    # droits par tableau de bord : le tableau « Praticiens » n'est visible que des rôles Praticien et
    # Direction, le tableau « Direction » du seul rôle Direction (gouvernance du L1)
    "DASHBOARD_RBAC": True,
    # filtres natifs affichés en barre horizontale au-dessus des graphiques (plus lisible en soutenance)
    "HORIZONTAL_FILTER_BAR": True,
}

# Nombres au format français dans les graphiques : 1 027 157 et 12,5
D3_FORMAT = {"decimal": ",", "thousands": " ", "grouping": [3], "currency": ["", " €"]}

# Palette des pyramides des âges : 1re série (femmes) en rose, 2e (hommes) en bleu (Okabe-Ito,
# lisible par les daltoniens). Les couleurs par libellé du tableau de bord ne s'appliquent pas aux
# séries issues de métriques : on passe donc par une palette dédiée choisie dans le graphique.
EXTRA_CATEGORICAL_COLOR_SCHEMES = [
    {"id": "chu_femmes_hommes", "description": "Femmes / Hommes", "label": "CHU - Femmes / Hommes",
     "isDefault": False, "colors": ["#CC79A7", "#0072B2", "#999999"]},
]

# API REST de gestion des rôles et des comptes (utilisée par le script de construction)
FAB_ADD_SECURITY_API = True

# Cache des résultats des graphiques dans la base de métadonnées (pas de Redis dans la stack) :
# un graphique déjà calculé s'affiche immédiatement ; « Forcer l'actualisation » le recalcule.
# Durée : 1 jour, les données ne changent qu'au chargement (DAG chu_pipeline).
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "SupersetMetastoreCache",
    "CACHE_KEY_PREFIX": "chu_data_",
    "CACHE_DEFAULT_TIMEOUT": 86400,
}
FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "SupersetMetastoreCache",
    "CACHE_KEY_PREFIX": "chu_filtre_",
    "CACHE_DEFAULT_TIMEOUT": 86400,
}
EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "SupersetMetastoreCache",
    "CACHE_KEY_PREFIX": "chu_explore_",
    "CACHE_DEFAULT_TIMEOUT": 86400,
}

# SQL Lab (démonstration « requête en direct ») : le compte bi est en lecture seule côté PostgreSQL,
# Superset bloque en plus toute instruction autre que SELECT / EXPLAIN sur la connexion « CHU Gold ».
SQLLAB_TIMEOUT = 120
SQL_MAX_ROW = 100000
