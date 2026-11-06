# Couche Silver — projet Cloud Healthcare Unit

Jobs Spark **Bronze → Silver** du Lakehouse (Livrable 1 : NiFi → Bronze S3,
Spark → Silver Iceberg, Spark → Gold PostgreSQL).

La couche Silver produit les 9 tables du **contrat Silver → staging** attendues
par le Gold : données typées, nettoyées, dédoublonnées et pseudonymisées, avec
les lignes non conformes mises en quarantaine.

**État** : 14 tests passent. La chaîne a tourné sur les données réelles du
dossier DATA_2, rangées comme NiFi les dépose (8 tables sur 9, en 7 min 53 s
sur 2 cœurs). Le rapport est dans `docs/rapport_qualite_donnees_reelles.md`.
Le code suit le contrat Silver → staging validé avec le Gold le 2 novembre.

## Arborescence

```
silver_cli.py              point d'entrée spark-submit (run | ddl | qualite | export)
sql/silver_ddl.sql         DDL des tables Iceberg (partitionnement justifié en tête)
src/silver/
  config.py                chemins + correspondance des colonnes sources (vérifiée sur DATA_2)
  contract.py              contrat Silver -> staging (colonnes, types, clés)
  common.py                lecture Bronze (CSV, xlsx, Parquet), normalisation, HMAC, quarantaine
  store.py                 écriture Iceberg (MERGE / overwrite) ou Parquet (tests)
  run.py                   orchestration d'un job : seuil 5 %, journal
  jobs/*.py                un job par table (règles en tête de chaque fichier)
  quality_report.py        rapport de qualité (attendu du Livrable 2)
  export_staging.py        copie Silver -> schéma staging PostgreSQL
tests/
  fixtures_bronze.py       jeu fictif au format exact des données réelles, avec cas sales
  test_pipeline.py         14 tests bout en bout
docs/
  rapport_qualite_donnees_reelles.md
```

## Ordre d'exécution

`communes → etablissements → professionnels → diagnostics → patients →
consultations → hospitalisations → satisfaction → deces`

## Ce que le Bronze doit déposer (rangement NiFi de la branche feat/Nifi)

NiFi range chaque fichier sous `bronze/<source>/<AAAA-MM-JJ>/<fichier>`, avec un
manifeste `<fichier>.manifest.json` qui contient son empreinte SHA-256. Silver
lit ce rangement tel quel :
- la date d'ingestion est lue dans le chemin ;
- les manifestes ne sont jamais lus comme données ;
- si un manifeste existe, l'empreinte du fichier est recalculée. Un fichier
  modifié est refusé en entier (contrôle d'entrée du Livrable 1, désactivable
  avec `SILVER_VERIFY_MANIFEST=0`).

Plusieurs fichiers peuvent partager un dossier de source : Silver les distingue
par leur nom (`include` dans `config.py`). Le format est déduit de l'extension.

| Dossier Bronze (`<source>`) | Fichiers attendus (nom) | État côté Bronze |
|---|---|---|
| `s1_hospitalisations` | `Hospitalisations.csv` | **branché** (flux NiFi existant) |
| `s2_etablissements` | `etablissement_sante.csv`, `activite_professionnel_sante.csv` | prévu |
| `s3_satisfaction` | dossier `Satisfaction/` tel quel (seuls les fichiers e-Satis sont lus) | prévu |
| `s4_deces` | `deces.csv` | prévu |
| `s1_soins` | une table par fichier : `Patient`, `Consultation`, `Diagnostic`, `Professionnel_de_sante`, `Specialites`, `Prescription` (`.avro` NiFi ; `.parquet` ou `.csv` acceptés) | prévu |
| `s0_cog` | `v_commune_AAAA.csv`, `v_departement_AAAA.csv`, `v_region_AAAA.csv`, `v_mvt_commune_AAAA.csv` (insee.fr) | flux NiFi `bronze_s0_cog` |

Si le Bronze choisit d'autres noms de dossiers, il suffit de changer `path` dans
`src/silver/config.py`. La base PostgreSQL arrive en Avro (NiFi ExecuteSQL, types
logiques activés) : l'image Spark doit contenir `spark-avro`. Les colonnes `date`
arrivent typées, les colonnes `time` sont remises en texte `HH:mm:ss` à la lecture.

`professionnel_sante.csv` n'est pas utilisé : la table `Professionnel_de_sante`
du dump contient les mêmes identifiants et se relie à `Specialites`.

## Constats sur les données réelles (ce qui a changé dans le code)

| Constat | Traitement |
|---|---|
| `Patient.Date` au format **américain** `M/d/yyyy` (`7/25/2013`) | format imposé pour cette source ; aucun repli jour/mois |
| `etablissement_sante.finess_site` vide à 99,95 % | FINESS déduit de `identifiant_organisation` = `F` + FINESS (+ parfois une date JJMMAAAA). Vérifié : 99 % des `finess_geo` e-Satis retrouvés. `R…` (cabinets RPPS) et `C…` (SIRET) n'ont pas de FINESS |
| Lieu d'exercice dans `activite_professionnel_sante.csv` | 64 % des consultations ont une organisation, **37 % seulement un FINESS** (le reste, ce sont des cabinets) → les faits sont rattachés par `id_organisation` (décision 7 du contrat) |
| Hospitalisations : CSV séparé (question 4), dates `JJ/MM/AAAA`, durée en jours, pas de date de sortie | date de sortie = entrée + durée ; organisation inconnue → NULL (non bloquant) |
| e-Satis : `finess` = **juridique**, `finess_geo` = site | `finess_geo` utilisé |
| e-Satis : 2017 et 2019 en CSV Windows-1252, 2020 en **xlsx** ; décimales mixtes `75,09` / `71.36` | lecture xlsx (openpyxl), virgule → point |
| e-Satis CA : autres rubriques (ACC, CER, OVS, PEC, AVH) | ACC → accueil, CER → chambre, OVS → sortie ; PEC et AVH sans colonne dans le contrat |
| Dossier Satisfaction : indicateurs IQSS (RCP, DPA, DAN…), lexiques, PDF | ignorés (filtre sur le nom de fichier) |
| Consultations de 20:00 à 02:00 | passage de minuit : 360 min |
| `deces.csv` : CSV avec en-tête `nom,prenom,sexe,date_naissance,…,code_lieu_deces,numero_acte_deces` | dates acceptées en AAAAMMJJ, AAAA-MM-JJ ou JJ/MM/AAAA ; code commune à 4 chiffres (zéro perdu par Excel) complété |

**Points d'attention à signaler au groupe :**
- `deces.csv` et `Professionnel_de_sante` font exactement **1 048 576 lignes**,
  la limite d'Excel. Ils ont probablement été tronqués en passant par Excel :
  prenez le fichier d'origine de l'INSEE pour les décès.
- 24 145 consultations (2,4 %) ont lieu **avant la naissance** du patient :
  données synthétiques incohérentes. Elles sont gardées, avec un âge NULL.
- `region_source` (e-Satis) s'écrit différemment selon les années
  (`Hauts-de-France` / `Hauts de France`). Pour le besoin B8, le Gold doit
  passer par FINESS → commune → COG, pas par cette colonne.

## Lancer les tests

```bash
pip install -r requirements.txt
python -m pytest -q tests          # environ 5 minutes, Spark local
make demo                          # chaîne complète sur le jeu fictif + rapport dans /tmp/demo
```

## Lancer sur la VM

```bash
pip install openpyxl                       # lecture des résultats e-Satis 2020 (.xlsx), sur le driver
make zip                                   # dist/silver.zip
export SILVER_HMAC_KEY="$(bao kv get -field=cle secret/chu/pseudonymisation)"   # depuis OpenBao
export BRONZE_ROOT=s3a://bronze SILVER_STORAGE=iceberg SILVER_CATALOG=lake

SPARK_CONF="--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.4 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hadoop \
  --conf spark.sql.catalog.lake.warehouse=s3a://silver/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://seaweedfs:8333 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.access.key=$S3_KEY --conf spark.hadoop.fs.s3a.secret.key=$S3_SECRET \
  --driver-memory 4g --py-files dist/silver.zip"

spark-submit $SPARK_CONF silver_cli.py ddl                     # une seule fois
spark-submit $SPARK_CONF silver_cli.py run --job all           # ou --job consultations --ingest-date 2026-11-02
spark-submit $SPARK_CONF silver_cli.py qualite --out docs/rapport_qualite.md
PG_URL=jdbc:postgresql://pg:5432/chu PG_USER=... PG_PASSWORD=... \
spark-submit $SPARK_CONF silver_cli.py export --tables all
```

Les versions des paquets (`3.5_2.12`, `hadoop-aws`) doivent correspondre à la
version de Spark de la VM. Pour Spark 4.0 : `iceberg-spark-runtime-4.0_2.13:1.10.0`.
La clé n'est jamais écrite dans le code ni dans Git.

Pendant le job satisfaction, Spark peut afficher des `InternalCompilerException`
(« generated.java ») : le plan est trop gros pour la génération de code, Spark
repasse automatiquement en mode interprété. Le job réussit quand même (vu sur les
données réelles).

## Règles communes (Livrable 1, spécifications ETL)

| Règle | Mise en œuvre |
|---|---|
| Valeurs nulles | 3 cas : normale (gardée), remplaçable (→ `NULL`, le Gold pose « Inconnu »), bloquante (→ rejet) |
| Quarantaine | table `quarantaine` : table, catégorie, motif, clé, fichier, n° de ligne. **Pas de contenu brut** (il contiendrait des noms) : l'original se retrouve en Bronze |
| Catégories | `rejet` (compte dans le seuil), `doublon` (version écartée), `hors_perimetre` (décès à l'étranger, établissement hors référentiel) |
| Seuil 5 % | si plus de 5 % de rejets, le job échoue **avant** d'écrire en Silver (`SILVER_REJECT_THRESHOLD`) |
| Doublons | lignes identiques fusionnées ; même clé et valeurs différentes : selon la table |
| Pseudonymisation | HMAC-SHA256, même clé pour toutes les tables, hexadécimal sur 64 caractères ; patients et professionnels ; calculée une fois par valeur distincte |
| Minimisation | noms, adresses, e-mail, téléphone, NIR, date de naissance complète, lieu de naissance : jamais écrits en Silver. Colonnes de travail filtrées par liste blanche (`run.TECH_COLS`) |
| Rejeu | `MERGE` sur la clé métier : relancer un job ne crée pas de doublon (testé) |
| Journal | table `journal_executions` : lignes lues / écrites / rejetées par exécution |

## Décisions du contrat (validées avec le Gold) et mise en œuvre

| Décision | Côté Silver |
|---|---|
| 1. COG ingéré par Bronze, nettoyé par Silver ; décès à l'étranger en quarantaine | `communes` + `passage_communes` ; codes 99xxx en `hors_perimetre` |
| 2. `id_professionnel` pseudonymisé, `type_identifiant` conservé | HMAC-SHA256, même clé que les patients |
| 3. Établissement d'une consultation déduit en Silver, NULL si introuvable | lieu d'exercice, établissement `F…` prioritaire |
| 4. Hospitalisations : source à confirmer | **CSV `Hospitalisations.csv`** (absent de la base PostgreSQL) |
| 5. MERGE (consultations, hospitalisations, décès), remplacement complet (établissements, satisfaction, COG), staging rechargé en entier | `MODE` de chaque job ; établissements et satisfaction relisent toutes les ingestions, donc l'historique SCD2 est conservé |
| 6. Patients : département de résidence | `code_departement` (2A/2B, 971 à 976) |
| 7. Faits rattachés par `id_organisation` | `id_organisation` dans consultations et hospitalisations, contrôlé contre le référentiel |
| Point ouvert `date_partielle` | **abandonné** (décision du Gold : indicateurs annuels) ; 00 → 1er janvier |

Professionnels, diagnostics et patients ne sont pas cités dans la décision 5 :
ils restent en `MERGE`.

## Limites connues

- **Testé** : toutes les règles (14 tests, dont le refus d'un fichier falsifié)
  et la chaîne sur les données réelles rangées comme NiFi les dépose, sauf les
  décès, en mode Parquet (Spark 4.0.1). La DDL et le `MERGE` passent
  l'analyseur SQL de Spark.
- **Non testé** : `deces.csv` réel (seul l'en-tête est connu : le format des
  dates est géré sous trois formes, à vérifier) ; exécution sur Iceberg /
  SeaweedFS ; export JDBC.
- Professionnel avec plusieurs lieux d'exercice : un établissement (`F…`) est
  retenu en priorité, puis le plus petit identifiant. Choix déterministe, pas
  forcément le vrai lieu de chaque consultation.
- Le compteur « lues » du journal ne compte pas les doublons stricts fusionnés.
