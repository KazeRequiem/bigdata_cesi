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
