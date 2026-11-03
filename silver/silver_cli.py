"""Point d'entrée pour spark-submit (qui ne sait pas lancer un module avec -m).

    spark-submit [conf] --py-files dist/silver.zip silver_cli.py run --job all
    spark-submit [conf] --py-files dist/silver.zip silver_cli.py ddl
    spark-submit [conf] --py-files dist/silver.zip silver_cli.py qualite --out rapport_qualite.md
    spark-submit [conf] --py-files dist/silver.zip silver_cli.py export --tables all
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

COMMANDS = {
    "run": "silver.run",
    "ddl": "silver.ddl",
    "qualite": "silver.quality_report",
    "export": "silver.export_staging",
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(__doc__)
        sys.exit(2)
    module = __import__(COMMANDS[sys.argv[1]], fromlist=["main"])
    sys.exit(module.main(sys.argv[2:]))
