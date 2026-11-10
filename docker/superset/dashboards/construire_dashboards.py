#!/usr/bin/env python3
"""Construit les tableaux de bord du Livrable 3 dans Superset (API REST), à partir de la couche Gold.

    docker compose exec superset python /app/dashboards/construire_dashboards.py

Prérequis : sql/13_vues_dashboard.sql exécuté et vues rafraîchies (sql/14), Superset démarré.
Relançable à volonté : jeux de données, graphiques et tableaux de bord du Livrable 3 sont MIS À JOUR
EN PLACE (repérés par la balise [L3:...] de leur description et par leur slug) : leurs identifiants et
leurs URL ne changent pas, un onglet resté ouvert continue de fonctionner. Seuls les graphiques que le
script n'utilise plus sont supprimés. Rien d'autre n'est touché dans Superset.

Ce qui est créé :
  - 6 jeux de données (vues gold.mv_* et gold.v_satisfaction) avec leurs métriques en SQL
    (définies une seule fois : couche sémantique) ;
  - 3 tableaux de bord : Vue d'ensemble, Praticiens, Direction (onglets + filtres natifs) ;
  - les rôles Praticien et Direction (droits par tableau de bord et par jeu de données) et, si
    SUPERSET_DEMO_PASSWORD est défini, les comptes praticien_demo et direction_demo ;
  - des requêtes enregistrées dans SQL Lab pour la démonstration « requête en direct ».
Les textes « Ce qu'il faut retenir » sont calculés sur les données au moment de la construction.
"""
import json
import os
import sys

import psycopg2
import requests

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088")
NOM_BASE = "CHU Gold"
SCHEMA = "gold"
ANNEE_DECES = 2019          # B7 : année demandée par l'énoncé
ANNEE_SATISFACTION = 2020   # B8 : année demandée par l'énoncé
SEUIL_PATIENTS = 10         # gouvernance : aucun professionnel affiché sous 10 patients distincts

# Palette dédiée aux pyramides (déclarée dans superset_config.py : femmes en rose, hommes en bleu)
PALETTE_SEXE = "chu_femmes_hommes"

# Couleurs (palette Okabe-Ito, lisible par les daltoniens)
COULEURS = {"Femme": "#CC79A7", "Homme": "#0072B2", "Indéterminé": "#999999",
            "Femmes": "#CC79A7", "Hommes": "#0072B2",
            "Avant (gold_v0)": "#999999", "Après (gold)": "#009E73", "Vue BI (gold.mv_*)": "#0072B2",
            "Établissement FINESS": "#009E73", "Autre structure (cabinet, centre)": "#E69F00",
            "Non rattaché": "#999999"}


# ---------------------------------------------------------------------------------------------
# Accès à l'API
# ---------------------------------------------------------------------------------------------
class Api:
    def __init__(self):
        self.s = requests.Session()
        r = self.s.post(f"{BASE}/api/v1/security/login", json={
            "username": os.environ["SUPERSET_ADMIN_USER"], "password": os.environ["SUPERSET_ADMIN_PASSWORD"],
            "provider": "db", "refresh": True})
        r.raise_for_status()
        self.s.headers["Authorization"] = "Bearer " + r.json()["access_token"]
        csrf = self.s.get(f"{BASE}/api/v1/security/csrf_token/").json()["result"]
        self.s.headers.update({"X-CSRFToken": csrf, "Referer": BASE})

    def _check(self, r, quoi):
        if r.status_code >= 400:
            sys.exit(f"ERREUR {quoi} : {r.status_code} {r.text[:800]}")
        return r.json() if r.text else {}

    def get(self, path, **params):
        return self._check(self.s.get(f"{BASE}/api/v1/{path}", params=params), f"GET {path}")

    def post(self, path, body):
        return self._check(self.s.post(f"{BASE}/api/v1/{path}", json=body), f"POST {path}")

    def put(self, path, body):
        return self._check(self.s.put(f"{BASE}/api/v1/{path}", json=body), f"PUT {path}")

    def delete(self, path):
        return self._check(self.s.delete(f"{BASE}/api/v1/{path}"), f"DELETE {path}")

    def tous(self, ressource, filtres=""):
        """Liste complète d'une ressource (pagination de l'API)."""
        res, page = [], 0
        while True:
            q = f"(page:{page},page_size:100{(',filters:!(' + filtres + ')') if filtres else ''})"
            lot = self.get(f"{ressource}/", q=q)["result"]
            res += lot
            if len(lot) < 100:
                return res
            page += 1


# ---------------------------------------------------------------------------------------------
# Chiffres clés calculés dans Gold (compte bi, lecture seule) pour les textes du récit
# ---------------------------------------------------------------------------------------------
def nb(x, dec=0):
    """Nombre au format français : 1 027 157 / 12,5."""
    s = f"{x:,.{dec}f}".replace(",", " ").replace(".", ",")
    return s.replace(" ", " ")


def chiffres_cles():
    cx = psycopg2.connect(host="postgres", dbname=os.environ["POSTGRES_DB"],
                          user=os.environ["BI_USER"], password=os.environ["BI_PASSWORD"])
    cur = cx.cursor()

    def un(sql):
        cur.execute(sql)
        return cur.fetchone()

    def tous(sql):
        cur.execute(sql)
        return cur.fetchall()

    c = {}
    c["consult"], c["patients"], c["annee_min"], c["annee_max"] = un(
        "SELECT count(*), count(DISTINCT sk_patient), min(annee), max(annee) FROM gold.mv_consultations")
    c["rattachement"] = dict((k, float(v)) for k, v in tous(
        "SELECT type_rattachement, round(100.0 * count(*) / sum(count(*)) OVER (), 1) "
        "FROM gold.mv_consultations GROUP BY 1"))
    c["top_chap_consult"] = un(
        "SELECT chapitre_diagnostic, count(*), round(100.0 * count(*) / sum(count(*)) OVER (), 1) "
        "FROM gold.mv_consultations GROUP BY 1 ORDER BY 2 DESC LIMIT 1")
    c["motif_consult"], = un(
        "SELECT round(100.0 * count(*) FILTER (WHERE motif = 'Consultation') / count(*), 1) FROM gold.mv_consultations")
    c["sejours"], c["duree_sejour"], c["hosp_min"], c["hosp_max"] = un(
        "SELECT count(*), round(avg(duree_sejour_jours), 1), min(annee), max(annee) FROM gold.mv_hospitalisations")
    c["top_chap_hosp"] = un(
        "SELECT chapitre_diagnostic, count(*), round(100.0 * count(*) / sum(count(*)) OVER (), 1) "
        "FROM gold.mv_hospitalisations GROUP BY 1 ORDER BY 2 DESC LIMIT 1")
    c["taux_hosp"] = un(
        "SELECT round(100.0 * sum(patients_hospitalises) / sum(patients_pris_en_charge), 2) "
        "FROM gold.mv_taux_hospitalisation WHERE annee BETWEEN 2015 AND 2021")[0]
    c["taux_hosp_age"] = tous(
        "SELECT tranche_age, round(100.0 * sum(patients_hospitalises) / sum(patients_pris_en_charge), 2) "
        "FROM gold.mv_taux_hospitalisation WHERE annee BETWEEN 2015 AND 2021 AND tranche_age <> 'Inconnu' "
        "GROUP BY 1 ORDER BY 2 DESC")
    c["deces_annee"], c["age_deces"] = un(
        f"SELECT sum(nb_deces), round(sum(somme_ages)::numeric / sum(nb_deces_age_connu), 1) "
        f"FROM gold.mv_deces WHERE annee = {ANNEE_DECES}")
    c["deces_annee_max"] = un("SELECT max(annee), sum(nb_deces) FILTER (WHERE annee = (SELECT max(annee) FROM gold.mv_deces)) FROM gold.mv_deces")
    regions = tous(
        f"SELECT region, sum(nb_deces), round(100000.0 * sum(nb_deces) / max(population_region), 0), "
        f"       round(sum(somme_ages)::numeric / NULLIF(sum(nb_deces_age_connu), 0), 1) "
        f"FROM gold.mv_deces WHERE annee = {ANNEE_DECES} AND population_region IS NOT NULL "
        f"GROUP BY 1")
    par_nombre = sorted(regions, key=lambda r: -r[1])
    par_taux = sorted(regions, key=lambda r: -r[2])
    c["deces_top_nombre"] = par_nombre[:3]
    c["deces_top_taux"] = par_taux[:3]
    c["deces_rang_taux"] = {r[0]: i + 1 for i, r in enumerate(par_taux)}
    c["deces_nb_regions"] = len(par_taux)
    c["deces_age_region"] = {r[0]: r[3] for r in regions}
    dep = tous(
        f"SELECT departement, round(100000.0 * sum(nb_deces) / max(population_departement), 0) "
        f"FROM gold.mv_deces WHERE annee = {ANNEE_DECES} AND population_departement IS NOT NULL "
        f"GROUP BY 1 ORDER BY 2 DESC")
    c["deces_dep_max"], c["deces_dep_min"] = dep[0], dep[-1]
    c["satisf"], c["recommandation"], c["reponses"] = un(
        f"SELECT round(sum(score_global * nb_reponses) / sum(nb_reponses), 1), "
        f"round(sum(taux_recommandation * nb_reponses) FILTER (WHERE taux_recommandation IS NOT NULL) / "
        f"      sum(nb_reponses) FILTER (WHERE taux_recommandation IS NOT NULL), 1), sum(nb_reponses) "
        f"FROM gold.v_satisfaction WHERE annee = {ANNEE_SATISFACTION}")
    sat = tous(
        f"SELECT region, round(sum(score_global * nb_reponses) / sum(nb_reponses), 1) "
        f"FROM gold.v_satisfaction WHERE annee = {ANNEE_SATISFACTION} GROUP BY 1 ORDER BY 2 DESC")
    c["satisf_regions"] = sat
    c["satisf_etab_faibles"] = un(
        f"SELECT count(*) FILTER (WHERE classement IN ('C', 'D')), count(*) FROM gold.v_satisfaction "
        f"WHERE annee = {ANNEE_SATISFACTION}")
    libelles_sous_scores = {"score_accueil": "l'accueil", "score_pec_infirmier": "la prise en charge infirmière",
                            "score_pec_medical": "la prise en charge médicale", "score_chambre": "la chambre",
                            "score_repas": "les repas", "score_sortie": "l'organisation de la sortie"}
    sous = []
    for col, lib in libelles_sous_scores.items():
        v = un(f"SELECT sum({col} * nb_reponses) / NULLIF(sum(nb_reponses) FILTER (WHERE {col} IS NOT NULL), 0) "
               f"FROM gold.v_satisfaction WHERE annee = {ANNEE_SATISFACTION}")[0]
        if v is not None:
            sous.append((lib, float(v)))
    sous.sort(key=lambda x: x[1])
    c["sous_scores"] = sous
    c["pros_seuil"], c["pros_total"] = un(
        f"SELECT count(*) FILTER (WHERE np >= {SEUIL_PATIENTS}), count(*) FROM ("
        f"  SELECT id_professionnel, count(DISTINCT sk_patient) AS np FROM gold.mv_consultations_pro "
        f"  WHERE id_professionnel <> 'INCONNU' GROUP BY 1) x")
    # mesures du benchmark (benchmark/mesurer.py), si elles existent
    cur.execute("SELECT to_regclass('gold.bench_resultats') IS NOT NULL")
    c["bench"] = {}
    if cur.fetchone()[0]:
        for req, conf, med in tous("SELECT requete, configuration, mediane_ms FROM gold.bench_resultats"):
            c["bench"].setdefault(req, {})[conf] = float(med)
    cx.close()
    return c


HAUTEUR_BANDEAU = 10  # une ligne de texte (unités de 8 px), deux sur un écran étroit


def bandeau(texte):
    """Une ligne « À retenir » en haut d'un onglet ; le détail du récit est porté à l'oral."""
    return ("md", f"**▶ À retenir** · {texte}")


def chapitre_court(libelle, n=45):
    """Libellé CIM-10 raccourci pour un bandeau : jusqu'à la première virgule, sinon coupé à n."""
    libelle = libelle.split(",")[0].strip()
    return libelle if len(libelle) <= n else libelle[:n - 1].rstrip() + "…"


def resume_benchmark(bench):
    """Deux chiffres pour le bandeau de l'onglet Performance."""
    av, ap, vb = "Avant (gold_v0)", "Après (gold)", "Vue BI (gold.mv_*)"
    morceaux = []
    if "B7" in bench and av in bench["B7"] and ap in bench["B7"]:
        b = bench["B7"]
        morceaux.append(f"B7 : {nb(b[av])} → {nb(b[ap])} ms" + (f" → {nb(b[vb], 1)} ms" if vb in b else ""))
    lents = [r for r, b in sorted(bench.items()) if av in b and ap in b and b[ap] > b[av] * 1.2 and b[av] >= 20]
    for r in lents:
        b = bench[r]
        morceaux.append(f"{r} ralenti par le partitionnement ({nb(b[av])} → {nb(b[ap])} ms)"
                        + (f", corrigé par la vue BI ({nb(b[vb])} ms)" if vb in b else ""))
    return " · ".join(morceaux) or "lancer benchmark/mesurer.py"


def lecture_benchmark(bench):
    """Texte d'interprétation des mesures, écrit à partir des chiffres (y compris quand l'optimisation
    n'apporte rien ou dégrade)."""
    if not bench:
        return "*Mesures absentes : lancer `benchmark/mesurer.py` puis reconstruire.*"
    av, ap, vb = "Avant (gold_v0)", "Après (gold)", "Vue BI (gold.mv_*)"
    lignes, petites, degradees = [], [], []
    for req in sorted(bench):
        b = bench[req]
        if av not in b or ap not in b:
            continue
        if max(b[av], b[ap]) < 20:
            petites.append(req)
            continue
        gain = b[av] / b[ap] if b[ap] else 0
        txt = f"**{req}** : {nb(b[av], 1)} → {nb(b[ap], 1)} ms"
        txt += f" (×{nb(gain, 1)})" if gain >= 1 else f" (**{nb(1 / gain, 1)} fois plus lent**)"
        if vb in b:
            txt += f" ; vue BI {nb(b[vb], 1)} ms"
        lignes.append("- " + txt)
        if gain < 1:
            degradees.append(req)
    texte = "\n".join(lignes)
    if petites:
        texte += (f"\n- {', '.join(petites)} : moins de 20 ms dans toutes les configurations ; tables trop "
                  "petites (milliers de lignes) pour que partition ou index changent quelque chose.")
    if degradees:
        texte += (f"\n- {', '.join(degradees)} : le partitionnement **dégrade** cette requête : le plan choisi "
                  "sur la table partitionnée est moins efficace pour la jointure avec le million de professionnels "
                  "(à montrer avec EXPLAIN) ; la vue BI, déjà jointe, corrige l'écart.")
    return texte


# ---------------------------------------------------------------------------------------------
# Jeux de données et métriques (couche sémantique)
# ---------------------------------------------------------------------------------------------
def m(nom, expr, libelle, fmt=",d", desc=""):
    return {"metric_name": nom, "expression": expr, "verbose_name": libelle, "d3format": fmt,
            "description": desc}


TAUX_RATTACH = "100.0 * COUNT(*) FILTER (WHERE sk_etablissement <> -1) / NULLIF(COUNT(*), 0)"


def score(col):
    return (f"SUM({col} * nb_reponses) / NULLIF(SUM(CASE WHEN {col} IS NOT NULL THEN nb_reponses END), 0)")


JEUX = {
    "mv_consultations": {
        "description": "Consultations à plat (B1, B2). Une ligne = une consultation. Sans identifiant de professionnel.",
        "metriques": [
            m("nb_consultations", "COUNT(*)", "Consultations"),
            m("nb_patients", "COUNT(DISTINCT sk_patient)", "Patients distincts"),
            m("consult_par_patient", "COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT sk_patient), 0)",
              "Consultations par patient", ",.2f"),
            m("duree_moyenne", "AVG(duree_minutes)", "Durée moyenne (min)", ",.1f"),
            m("nb_etablissements", "COUNT(DISTINCT sk_etablissement) FILTER (WHERE sk_etablissement <> -1)",
              "Établissements actifs"),
            m("taux_rattachement", TAUX_RATTACH, "Consultations rattachées à un établissement (%)", ",.1f",
              "Part des consultations dont l'établissement est connu (limite de la source : 36 % sans établissement)."),
        ],
    },
    "mv_consultations_pro": {
        "description": "B6 : consultations par professionnel (pseudonymisé). Réservé au rôle Direction.",
        "metriques": [
            m("nb_consultations", "COUNT(*)", "Consultations"),
            m("nb_patients", "COUNT(DISTINCT sk_patient)", "Patients distincts"),
        ],
    },
    "mv_hospitalisations": {
        "description": "Séjours hospitaliers à plat (B4, B5). Une ligne = un séjour.",
        "metriques": [
            m("nb_sejours", "COUNT(*)", "Séjours"),
            m("nb_patients_hosp", "COUNT(DISTINCT sk_patient)", "Patients hospitalisés"),
            m("duree_moyenne_sejour", "AVG(duree_sejour_jours)", "Durée moyenne de séjour (jours)", ",.1f"),
            m("sejours_femmes", "SUM(CASE WHEN sexe = 'Femme' THEN 1 ELSE 0 END)", "Femmes"),
            m("sejours_hommes_neg", "-SUM(CASE WHEN sexe = 'Homme' THEN 1 ELSE 0 END)", "Hommes",
              desc="Valeur négative : sert à tracer la pyramide des âges (hommes à gauche)."),
        ],
    },
    "mv_taux_hospitalisation": {
        "description": "B3, B5 : patients pris en charge et hospitalisés par année, sexe et tranche d'âge.",
        "metriques": [
            m("taux_hospitalisation",
              "100.0 * SUM(patients_hospitalises) / NULLIF(SUM(patients_pris_en_charge), 0)",
              "Taux d'hospitalisation (%)", ",.2f",
              "Patients hospitalisés / patients pris en charge (au moins une consultation ou un séjour dans l'année)."),
            m("patients_pris_en_charge", "SUM(patients_pris_en_charge)", "Patients pris en charge"),
            m("patients_hospitalises", "SUM(patients_hospitalises)", "Patients hospitalisés"),
        ],
    },
    "mv_deces": {
        "description": "B7 : décès agrégés par année, département, sexe et âge (lieu du décès), avec la population INSEE 2019 de la région.",
        "metriques": [
            m("nb_deces", "SUM(nb_deces)", "Décès"),
            m("taux_deces_100k", "100000.0 * SUM(nb_deces) / NULLIF(MAX(population_region), 0)",
              "Décès pour 100 000 habitants", ",.0f",
              "À utiliser regroupé PAR RÉGION (population légale INSEE 2019 de la région ; Mayotte : 2017)."),
            m("taux_deces_dep_100k", "100000.0 * SUM(nb_deces) / NULLIF(MAX(population_departement), 0)",
              "Décès pour 100 000 habitants (département)", ",.0f",
              "À utiliser regroupé PAR DÉPARTEMENT (population légale INSEE 2019 du département)."),
            m("population_region", "MAX(population_region)", "Population de la région (INSEE 2019)"),
            m("age_moyen_deces", "SUM(somme_ages) * 1.0 / NULLIF(SUM(nb_deces_age_connu), 0)",
              "Âge moyen au décès", ",.1f"),
            m("deces_femmes", "SUM(CASE WHEN sexe = 'Femme' THEN nb_deces ELSE 0 END)", "Femmes"),
            m("deces_hommes_neg", "-SUM(CASE WHEN sexe = 'Homme' THEN nb_deces ELSE 0 END)", "Hommes",
              desc="Valeur négative : sert à tracer la pyramide des âges (hommes à gauche)."),
        ],
    },
    "v_satisfaction": {
        "description": "B8 : résultats e-Satis par établissement et par année (scores sur 100).",
        "metriques": [
            m("score_global", score("score_global"), "Score global (sur 100)", ",.1f",
              "Moyenne pondérée par le nombre de répondants."),
            m("score_accueil", score("score_accueil"), "Accueil", ",.1f"),
            m("score_pec_infirmier", score("score_pec_infirmier"), "Prise en charge infirmière", ",.1f"),
            m("score_pec_medical", score("score_pec_medical"), "Prise en charge médicale", ",.1f"),
            m("score_chambre", score("score_chambre"), "Chambre", ",.1f"),
            m("score_repas", score("score_repas"), "Repas", ",.1f"),
            m("score_sortie", score("score_sortie"), "Organisation de la sortie", ",.1f"),
            m("taux_recommandation", score("taux_recommandation"), "Recommandation (%)", ",.1f"),
            m("nb_reponses", "SUM(nb_reponses)", "Répondants"),
            m("nb_etablissements", "COUNT(DISTINCT finess_site)", "Établissements évalués"),
        ],
    },
}

LIBELLES_COLONNES = {
    "annee": "Année", "date_annee": "Année (date)", "region": "Région", "departement": "Département",
    "etablissement": "Établissement", "finess_site": "FINESS", "type_rattachement": "Rattachement",
    "chapitre_diagnostic": "Chapitre CIM-10", "diagnostic": "Diagnostic", "specialite": "Spécialité",
    "sexe": "Sexe", "tranche_age": "Tranche d'âge", "tranche_age_10": "Tranche d'âge (10 ans)",
    "motif": "Motif", "id_professionnel": "Pseudonyme du professionnel", "code_professionnel": "Professionnel (pseudonyme)", "type_identifiant": "Type d'identifiant",
    "enquete": "Enquête", "classement": "Classement HAS", "nb_reponses": "Répondants",
    "duree_sejour_jours": "Durée de séjour (jours)", "age": "Âge",
}


def preparer_jeux(api, base_id):
    existants = {d["table_name"]: d["id"] for d in api.tous("dataset", f"(col:schema,opr:eq,value:{SCHEMA})")}
    ids = {}
    for table, spec in JEUX.items():
        if table in existants:
            ds_id = existants[table]
            api.put(f"dataset/{ds_id}/refresh", {})          # relit les colonnes de la vue
        else:
            ds_id = api.post("dataset/", {"database": base_id, "schema": SCHEMA, "table_name": table})["id"]
        ds = api.get(f"dataset/{ds_id}")["result"]
        colonnes = []
        for col in ds["columns"]:
            nom = col["column_name"]
            colonnes.append({
                "id": col["id"], "column_name": nom,
                "verbose_name": LIBELLES_COLONNES.get(nom),
                "is_dttm": nom in ("date_annee", "date_consultation", "date_entree"),
                "groupby": True, "filterable": True, "type": col.get("type"),
                "expression": col.get("expression") or "", "is_active": True,
            })
        anciennes = {x["metric_name"]: x["id"] for x in ds["metrics"]}
        metriques = []
        for mt in spec["metriques"]:
            mt = dict(mt)
            if mt["metric_name"] in anciennes:
                mt["id"] = anciennes[mt["metric_name"]]
            metriques.append(mt)
        api.put(f"dataset/{ds_id}?override_columns=true", {
            "description": spec["description"],
            "main_dttm_col": "date_annee" if any(x["column_name"] == "date_annee" for x in colonnes) else None,
            "columns": colonnes, "metrics": metriques, "cache_timeout": 86400,
        })
        ids[table] = ds_id
        print(f"  jeu de données {SCHEMA}.{table} : id {ds_id}")
    return ids


# ---------------------------------------------------------------------------------------------
# Graphiques
# ---------------------------------------------------------------------------------------------
def f_where(sql, nom="f"):
    return {"expressionType": "SQL", "sqlExpression": sql, "clause": "WHERE", "subject": None,
            "operator": None, "comparator": None, "isExtra": False, "isNew": False, "filterOptionName": nom}


def f_having(sql, nom="h"):
    d = f_where(sql, nom)
    d["clause"] = "HAVING"
    return d


def kpi(metrique, sous_titre, filtres=(), fmt=",d"):
    return {"viz_type": "big_number_total", "metric": metrique, "subheader": sous_titre,
            "adhoc_filters": list(filtres), "header_font_size": 0.4, "subheader_font_size": 0.15,
            "y_axis_format": fmt}


def barres(x, metriques, horizontal=True, groupby=(), limite=None, tri=None, tri_asc=None,
           filtres=(), empile=False, fmt=None, legende=False, titre_x="", titre_y="", inverser=True):
    """Barres. En horizontal, ECharts place la 1re catégorie en bas : `inverser` retourne l'ordre pour
    que le classement se lise de haut en bas (à désactiver pour une pyramide des âges)."""
    p = {"viz_type": "echarts_timeseries_bar", "x_axis": x, "metrics": list(metriques),
         "groupby": list(groupby), "adhoc_filters": list(filtres), "orientation":
         "horizontal" if horizontal else "vertical", "row_limit": 10000, "show_value": not groupby and not empile,
         "show_legend": legende or bool(groupby) or len(metriques) > 1, "legendOrientation": "top",
         "legendType": "scroll", "rich_tooltip": True, "truncateXAxis": True, "y_axis_bounds": [None, None],
         "x_axis_title": titre_x, "y_axis_title": titre_y, "x_axis_title_margin": 30, "y_axis_title_margin": 40,
         "y_axis_title_position": "Left", "color_scheme": "supersetColors", "zoomable": False,
         "only_total": True, "sort_series_type": "sum", "comparison_type": "values", "annotation_layers": [],
         "order_desc": True}
    p["y_axis_format"] = fmt or ",d"
    if empile:
        p["stack"] = "Stack"
    if limite:
        p["x_axis_sort"] = tri or metriques[0]
        p["x_axis_sort_asc"] = False
        p["series_limit"] = limite
        p["timeseries_limit_metric"] = tri or metriques[0]
        p["row_limit"] = limite
    if tri is not None or tri_asc is not None:
        p["x_axis_sort"] = tri or metriques[0]
        p["x_axis_sort_asc"] = bool(tri_asc)
    if horizontal and inverser and "x_axis_sort" in p:
        # en barres horizontales, la première catégorie est en bas : on inverse pour avoir la plus
        # grande valeur en haut
        p["x_axis_sort_asc"] = not p["x_axis_sort_asc"]
    return p


def libelle_court(colonne, n=42):
    """Colonne ad hoc : libellé coupé à n caractères pour tenir sur une ligne."""
    return {"expressionType": "SQL", "label": "Libellé",
            "sqlExpression": f"CASE WHEN length({colonne}) > {n} THEN left({colonne}, {n - 1}) || '…' ELSE {colonne} END"}


def part(libelle):
    """Métrique ad hoc : part de chaque ligne dans le total de la requête (filtres compris).
    La fonction de fenêtre est calculée après le GROUP BY et avant le LIMIT : le total porte donc sur
    toutes les lignes de la période filtrée, pas seulement sur les 15 affichées."""
    return {"expressionType": "SQL", "sqlExpression": "COUNT(*) * 1.0 / SUM(COUNT(*)) OVER ()",
            "label": libelle, "hasCustomLabel": True, "optionName": "part"}


def top(x, metrique, n=15, filtres=(), fmt=",d", plus_faibles=False):
    """Top n en barres horizontales, une ligne par entrée, valeur affichée au bout de la barre.
    plus_faibles=True : les n plus faibles, le plus faible en haut."""
    label = metrique["label"] if isinstance(metrique, dict) else metrique
    p = barres(x, [metrique], limite=n, filtres=filtres, fmt=fmt)
    p.update({"x_axis_sort": label, "timeseries_limit_metric": metrique, "truncateXAxis": False,
              "show_value": True, "y_axis_title": "", "x_axis_title": ""})
    if plus_faibles:
        p["order_desc"] = False         # la requête garde les n plus faibles
        p["x_axis_sort_asc"] = False    # affichage : le plus faible en haut
    return p


def courbe(metriques, groupby=(), filtres=(), fmt=None, x="date_annee", titre_y=""):
    p = {"viz_type": "echarts_timeseries_line", "x_axis": x, "time_grain_sqla": "P1Y",
         "metrics": list(metriques), "groupby": list(groupby), "adhoc_filters": list(filtres),
         "row_limit": 10000, "x_axis_time_format": "%Y", "rich_tooltip": True, "show_legend": True,
         "legendOrientation": "top", "legendType": "scroll", "markerEnabled": True, "markerSize": 6,
         "y_axis_bounds": [0, None], "truncateYAxis": False, "color_scheme": "supersetColors",
         "y_axis_title": titre_y, "y_axis_title_margin": 40, "y_axis_title_position": "Left",
         "comparison_type": "values", "annotation_layers": []}
    if fmt:
        p["y_axis_format"] = fmt
    return p


def tableau(groupby, metriques, pourcent=(), filtres=(), tri=None, asc=False, limite=10000, page=15,
            couleurs=None):
    """Tableau sans barres grises. couleurs = {métrique: couleur} : dégradé par valeur dans la colonne
    (foncé = élevé), pour comparer plusieurs indicateurs d'un coup d'œil."""
    return {"viz_type": "table", "query_mode": "aggregate", "groupby": list(groupby),
            "metrics": list(metriques), "percent_metrics": list(pourcent), "adhoc_filters": list(filtres),
            "timeseries_limit_metric": tri or metriques[0], "order_desc": not asc, "row_limit": limite,
            "server_pagination": False, "page_length": page, "include_search": False,
            "show_cell_bars": False, "align_pn": False, "color_pn": False, "allow_rearrange_columns": True,
            "table_timestamp_format": "smart_date",
            "conditional_formatting": [{"column": col, "colorScheme": coul, "operator": "None"}
                                       for col, coul in (couleurs or {}).items()]}


def carte(metrique, entite="code_carte", pays="france_regions", couleurs="oranges", filtres=(), fmt=",d"):
    return {"viz_type": "country_map", "select_country": pays, "entity": entite, "metric": metrique,
            "adhoc_filters": list(filtres), "row_limit": 50000, "linear_color_scheme": couleurs,
            "number_format": fmt}


def heatmap(x, y, metrique, filtres=()):
    return {"viz_type": "heatmap_v2", "x_axis": x, "groupby": y, "metric": metrique,
            "adhoc_filters": list(filtres), "row_limit": 10000, "linear_color_scheme": "schemeBlues",
            "normalize_across": "heatmap", "show_values": True, "show_legend": True,
            "sort_x_axis": "alpha_asc", "sort_y_axis": "value_desc", "left_margin": "auto",
            "bottom_margin": "auto", "xscale_interval": -1, "yscale_interval": -1, "value_bounds": [None, None],
            "y_axis_format": ",d"}


def anneau(groupby, metrique, filtres=()):
    return {"viz_type": "pie", "groupby": [groupby], "metric": metrique, "adhoc_filters": list(filtres),
            "row_limit": 100, "donut": True, "innerRadius": 40, "outerRadius": 70, "show_labels": True,
            "labels_outside": True, "label_line": True, "label_type": "key_percent", "show_legend": False,
            "number_format": ",d", "color_scheme": "supersetColors", "sort_by_metric": True}


def radar(groupby, metriques, filtres=()):
    """Radar. `metriques` = liste de (libellé, expression SQL) : le radar affiche le nom des métriques
    sur ses axes, d'où des métriques ad hoc avec un libellé lisible plutôt que les noms techniques."""
    adhoc = [{"expressionType": "SQL", "sqlExpression": expr, "label": lib, "hasCustomLabel": True,
              "optionName": f"radar_{i}"} for i, (lib, expr) in enumerate(metriques)]
    return {"viz_type": "radar", "groupby": [groupby], "metrics": adhoc,
            "adhoc_filters": list(filtres), "row_limit": 10, "show_legend": True, "legendOrientation": "top",
            "show_labels": False, "is_circle": False, "number_format": ",.1f",
            "column_config": {lib: {"radarMetricMaxValue": 100} for lib, _ in metriques}}


class Graphiques:
    """Crée ou met à jour les graphiques ; chacun est repéré par une clé (balise [L3:clé] dans sa
    description). Un graphique existant est MIS À JOUR (même identifiant) : un tableau de bord resté
    ouvert dans un navigateur, un lien ou un filtre continue de pointer vers le bon graphique."""

    def __init__(self, api, jeux, existants):
        self.api, self.jeux, self.ids, self.jeu_de = api, jeux, {}, {}
        self.existants = existants

    def ajouter(self, cle, titre, jeu, params, description=""):
        ds = self.jeux[jeu]
        params = dict(params, datasource=f"{ds}__table", slice_name=titre)
        corps = {"slice_name": titre, "viz_type": params["viz_type"], "datasource_id": ds,
                 "datasource_type": "table", "params": json.dumps(params),
                 "description": f"[L3:{cle}] {description}".strip(), "cache_timeout": 86400}
        if cle in self.existants:
            self.ids[cle] = self.existants[cle]
            self.api.put(f"chart/{self.ids[cle]}", corps)
        else:
            self.ids[cle] = self.api.post("chart/", corps)["id"]
        self.jeu_de[cle] = jeu
        return cle


def inventaire(api):
    """Graphiques du Livrable 3 déjà présents (clé -> id, + doublons éventuels) et tableaux chu-* (slug -> id)."""
    graphiques, doublons = {}, []
    for ch in api.tous("chart"):
        desc = api.get(f"chart/{ch['id']}")["result"].get("description") or ""
        if desc.startswith("[L3:") and "]" in desc:
            cle = desc[4:desc.index("]")]
            if cle in graphiques:
                doublons.append(ch["id"])
            else:
                graphiques[cle] = ch["id"]
    tableaux = {d["slug"]: d["id"] for d in api.tous("dashboard") if (d.get("slug") or "").startswith("chu-")}
    return graphiques, tableaux, doublons


def nettoyer(api, existants, utilises, doublons):
    """Supprime seulement les graphiques du Livrable 3 qui ne sont plus utilisés par le script."""
    obsoletes = [cid for cle, cid in existants.items() if cle not in utilises] + doublons
    for cid in obsoletes:
        api.delete(f"chart/{cid}")
    return len(obsoletes)


# ---------------------------------------------------------------------------------------------
# Mise en page : onglets > lignes > graphiques / textes
# ---------------------------------------------------------------------------------------------
class Page:
    """Construit position_json. Une ligne = liste de (clé du graphique | ('md', texte), largeur, hauteur)."""

    def __init__(self, titre, graphiques):
        self.g = graphiques
        self.pos = {"DASHBOARD_VERSION_KEY": "v2",
                    "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": titre}}}
        self.n = 0
        self.onglets = {}          # nom -> id
        self.cles_par_onglet = {}  # id onglet -> clés de graphiques
        self.racine_onglets = None

    def _id(self, prefixe):
        self.n += 1
        return f"{prefixe}-l3-{self.n}"

    def _lignes(self, parent_id, parents, lignes, cles):
        enfants = []
        for ligne in lignes:
            rid = self._id("ROW")
            items = []
            for elt, largeur, hauteur in ligne:
                if isinstance(elt, tuple) and elt[0] == "md":
                    iid = self._id("MARKDOWN")
                    self.pos[iid] = {"type": "MARKDOWN", "id": iid, "children": [],
                                     "parents": parents + [parent_id, rid],
                                     "meta": {"width": largeur, "height": hauteur, "code": elt[1]}}
                else:
                    iid = self._id("CHART")
                    cid = self.g.ids[elt]
                    cles.append(elt)
                    self.pos[iid] = {"type": "CHART", "id": iid, "children": [],
                                     "parents": parents + [parent_id, rid],
                                     "meta": {"chartId": cid, "width": largeur, "height": hauteur}}
                items.append(iid)
            self.pos[rid] = {"type": "ROW", "id": rid, "children": items, "parents": parents + [parent_id],
                             "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            enfants.append(rid)
        return enfants

    def grille(self, lignes):
        cles = []
        self.pos["ROOT_ID"] = {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]}
        self.pos["GRID_ID"] = {"type": "GRID", "id": "GRID_ID", "parents": ["ROOT_ID"],
                               "children": self._lignes("GRID_ID", ["ROOT_ID"], lignes, cles)}
        self.cles_par_onglet["ROOT_ID"] = cles

    def onglet(self, nom, lignes):
        if not self.racine_onglets:
            self.racine_onglets = "TABS-l3-racine"
            self.pos["ROOT_ID"] = {"type": "ROOT", "id": "ROOT_ID", "children": [self.racine_onglets]}
            self.pos[self.racine_onglets] = {"type": "TABS", "id": self.racine_onglets, "children": [],
                                             "parents": ["ROOT_ID"]}
        tid = self._id("TAB")
        cles = []
        parents = ["ROOT_ID", self.racine_onglets]
        self.pos[tid] = {"type": "TAB", "id": tid, "parents": parents, "meta": {"text": nom},
                         "children": self._lignes(tid, parents, lignes, cles)}
        self.pos[self.racine_onglets]["children"].append(tid)
        self.onglets[nom] = tid
        self.cles_par_onglet[tid] = cles
        return tid

    def toutes_les_cles(self):
        return [c for cles in self.cles_par_onglet.values() for c in cles]


def filtre(fid, nom, jeu_cible, colonne, page, onglets=None, exclure=(), defaut=None, multiple=True,
           description=""):
    """Filtre natif « liste de valeurs ». S'applique aux graphiques des onglets donnés (tous sinon)
    dont le jeu de données possède la colonne ; les clés de `exclure` sont écartées."""
    g = page.g
    onglets_ids = [page.onglets[o] for o in onglets] if onglets else list(page.cles_par_onglet)
    cles_portee = [c for t in onglets_ids for c in page.cles_par_onglet[t]]
    colonnes_ok = {j for j, ds in g.jeux.items() if colonne in COLONNES[j]}
    dans = [c for c in cles_portee if g.jeu_de[c] in colonnes_ok and c not in exclure]
    hors = [g.ids[c] for c in page.toutes_les_cles() if c not in dans]
    masque = {"extraFormData": {}, "filterState": {}, "ownState": {}}
    if defaut is not None:
        masque = {"extraFormData": {"filters": [{"col": colonne, "op": "IN", "val": defaut}]},
                  "filterState": {"value": defaut, "label": ", ".join(map(str, defaut))}, "ownState": {}}
    racine = onglets_ids if onglets else ["ROOT_ID"]
    return {
        "id": f"NATIVE_FILTER-{fid}", "name": nom, "filterType": "filter_select", "type": "NATIVE_FILTER",
        "targets": [{"datasetId": g.jeux[jeu_cible], "column": {"name": colonne}}],
        # Limite connue de Superset 4.1 : la liste accepte une saisie libre (taper « 1999 » puis Entrée crée
        # une valeur qui ne renvoie rien). searchAllOptions=True l'empêcherait, mais l'interface réenregistre
        # le tableau à chaque ouverture et remet cette option à False : on choisit donc dans la liste.
        "controlValues": {"enableEmptyFilter": False, "defaultToFirstItem": False, "multiSelect": multiple,
                          "searchAllOptions": False, "inverseSelection": False, "sortAscending": True},
        "defaultDataMask": masque, "cascadeParentIds": [],
        "scope": {"rootPath": racine, "excluded": sorted(set(hors))},
        "chartsInScope": sorted(g.ids[c] for c in dans), "tabsInScope": onglets_ids if onglets else [],
        "description": description,
    }


COLONNES = {}  # jeu -> colonnes, rempli au démarrage


def publier(api, page, titre, slug, filtres, roles, graphiques, tableaux_existants, description=""):
    # pas de "color_scheme" au niveau du tableau : il s'imposerait aux cartes (couleurs par catégorie au
    # lieu du dégradé) ; chaque graphique garde sa palette, label_colors fixe les couleurs Femme/Homme...
    meta = {"native_filter_configuration": filtres, "filter_bar_orientation": "HORIZONTAL",
            "label_colors": COULEURS, "shared_label_colors": {}, "refresh_frequency": 0,
            "expanded_slices": {}, "timed_refresh_immune_slices": [], "cross_filters_enabled": True,
            "chart_configuration": {}, "default_filters": "{}", "filter_scopes": {},
            "global_chart_configuration": {"scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                                           "chartsInScope": sorted(graphiques.ids[c] for c in page.toutes_les_cles())}}
    entete = {"dashboard_title": titre, "slug": slug, "published": True, "css": CSS,
              "json_metadata": json.dumps(meta)}
    if slug in tableaux_existants:          # mise à jour en place : l'URL et l'identifiant ne changent pas
        did = tableaux_existants[slug]
        api.put(f"dashboard/{did}", entete)
    else:
        did = api.post("dashboard/", entete)["id"]
    for cle in page.toutes_les_cles():
        cid = graphiques.ids[cle]
        actuels = [d["id"] for d in api.get(f"chart/{cid}")["result"].get("dashboards", [])]
        api.put(f"chart/{cid}", {"dashboards": sorted(set(actuels + [did]))})
    api.put(f"dashboard/{did}", {"position_json": json.dumps(page.pos), "json_metadata": json.dumps(meta),
                                 "roles": roles})
    print(f"  tableau de bord « {titre} » : {BASE}/superset/dashboard/{slug}/")
    return did


CSS = """
.dashboard-markdown { font-size: 14px; line-height: 1.45; }
.dashboard-markdown h3 { margin-top: 4px; }
.dashboard-markdown .encadre { border-left: 4px solid #0072B2; padding: 4px 10px; background: rgba(0,114,178,.06); }
"""


# ---------------------------------------------------------------------------------------------
# Rôles, comptes de démonstration (contexte applicatif Superset)
# ---------------------------------------------------------------------------------------------
def roles_et_comptes(jeux_praticien, jeux_direction):
    from superset.app import create_app
    app = create_app()
    with app.app_context():
        from superset import db, security_manager as sm
        from superset.connectors.sqla.models import SqlaTable
        gamma = sm.find_role("Gamma")
        ids = {}
        for nom, jeux in (("Praticien", jeux_praticien), ("Direction", jeux_direction)):
            role = sm.find_role(nom) or sm.add_role(nom)
            perms = list(gamma.permissions)
            for table in jeux:
                t = db.session.query(SqlaTable).filter_by(schema=SCHEMA, table_name=table).one()
                pv = sm.find_permission_view_menu("datasource_access", t.perm)
                if pv:
                    perms.append(pv)
            role.permissions = perms
            ids[nom] = role.id
        db.session.commit()
        mdp = os.environ.get("SUPERSET_DEMO_PASSWORD")
        if mdp:
            for login, nom, prenom in (("praticien_demo", "Praticien", "Démo"), ("direction_demo", "Direction", "Démo")):
                role = sm.find_role(nom)
                u = sm.find_user(username=login)
                if not u:
                    sm.add_user(login, prenom, nom, f"{login}@chu.local", [role], password=mdp)
                else:
                    from werkzeug.security import generate_password_hash
                    u.roles = [role]
                    u.password = generate_password_hash(mdp)
                    db.session.commit()
            print("  comptes praticien_demo et direction_demo prêts (mot de passe : SUPERSET_DEMO_PASSWORD)")
        else:
            print("  SUPERSET_DEMO_PASSWORD absent : comptes de démonstration non créés")
        return ids


# ---------------------------------------------------------------------------------------------
# Requêtes enregistrées pour la démonstration live (SQL Lab)
# ---------------------------------------------------------------------------------------------
REQUETES_DEMO = [
    ("Démo 1 - B7 sur la vue jointe (avant matérialisation)",
     "Jointure fait_deces (24,7 M de lignes) + dimensions refaite à chaque appel : quelques secondes.",
     f"SELECT libelle_region, sum(nb_deces) AS nb_deces\nFROM gold.v_deces_region\n"
     f"WHERE annee = {ANNEE_DECES}\nGROUP BY libelle_region\nORDER BY nb_deces DESC;"),
    ("Démo 2 - B7 sur la table de faits partitionnée (filtre sur la clé de partition)",
     "Le filtre sur sk_date_deces ne lit que la partition de l'année : élagage des partitions.",
     f"SELECT g.libelle_region, sum(f.nb_deces) AS nb_deces\nFROM gold.fait_deces f\n"
     f"JOIN gold.dim_geographie g ON g.sk_geo = f.sk_geo_deces\n"
     f"WHERE f.sk_date_deces BETWEEN {ANNEE_DECES}0101 AND {ANNEE_DECES}1231\n"
     f"GROUP BY g.libelle_region\nORDER BY nb_deces DESC;"),
    ("Démo 3 - B7 sur la vue matérialisée du tableau de bord (taux pour 100 000 hab.)",
     "Agrégat pré-calculé (gold.mv_deces) : quelques millisecondes ; c'est ce que lisent les cartes.",
     f"SELECT region, sum(nb_deces) AS nb_deces,\n"
     f"       round(100000.0 * sum(nb_deces) / max(population_region)) AS deces_pour_100000_hab\n"
     f"FROM gold.mv_deces\nWHERE annee = {ANNEE_DECES} AND population_region IS NOT NULL\n"
     f"GROUP BY region\nORDER BY deces_pour_100000_hab DESC;"),
    ("Démo 4 - Plan d'exécution : élagage des partitions",
     "EXPLAIN ANALYZE : une seule partition (fait_deces_2019) apparaît dans le plan.",
     f"EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)\nSELECT count(*)\nFROM gold.fait_deces\n"
     f"WHERE sk_date_deces BETWEEN {ANNEE_DECES}0101 AND {ANNEE_DECES}1231;"),
    ("Démo 5 - B8 satisfaction par région (année 2020)",
     "Score global pondéré par le nombre de répondants.",
     f"SELECT region, round(sum(score_global * nb_reponses) / sum(nb_reponses), 1) AS score_global,\n"
     f"       sum(nb_reponses) AS repondants\nFROM gold.v_satisfaction\nWHERE annee = {ANNEE_SATISFACTION}\n"
     f"GROUP BY region\nORDER BY score_global DESC;"),
]


def requetes_demo(api, base_id):
    for q in api.tous("saved_query"):
        if q["label"].startswith("Démo "):
            api.delete(f"saved_query/{q['id']}")
    for label, desc, sql in REQUETES_DEMO:
        api.post("saved_query/", {"db_id": base_id, "schema": SCHEMA, "label": label,
                                  "description": desc, "sql": sql})
    print(f"  {len(REQUETES_DEMO)} requêtes enregistrées dans SQL Lab (« Démo 1 » à « Démo 5 »)")


# ---------------------------------------------------------------------------------------------
# Les trois tableaux de bord
# ---------------------------------------------------------------------------------------------
def construire(api, jeux, c, existants):
    g = Graphiques(api, jeux, existants)
    hors_inconnu = f_where("sk_etablissement <> -1", "rattache")
    # la source ne contient aucun séjour après hosp_max : sans ce filtre, le taux tomberait à 0 en 2022-2023
    annees_hosp = f_where(f"annee BETWEEN {c['hosp_min']} AND {c['hosp_max']}", "annees_hosp")
    periode_hosp = f"{c['hosp_min']}-{c['hosp_max']}"
    annee_deces = f_where(f"annee = {ANNEE_DECES}", "annee_deces")
    annee_satisf = f_where(f"annee = {ANNEE_SATISFACTION}", "annee_satisf")
    region_connue = f_where("code_carte IS NOT NULL", "region_connue")
    pop_connue = f_where("population_region IS NOT NULL", "pop_connue")
    pct_finess = c["rattachement"].get("Établissement FINESS", 0)
    pct_sans = c["rattachement"].get("Non rattaché", 0)
    top_n, top_t = c["deces_top_nombre"], c["deces_top_taux"]
    rang_idf = c["deces_rang_taux"].get("Île-de-France")
    sat = c["satisf_regions"]
    metro = [r for r in sat if r[0] not in ("Guadeloupe", "Martinique", "Guyane", "La Réunion", "Mayotte")]

    # ---------------- Vue d'ensemble ----------------
    g.ajouter("ve_consult", "Consultations", "mv_consultations", kpi("nb_consultations", "consultations (période filtrée)"))
    g.ajouter("ve_patients", "Patients suivis", "mv_consultations", kpi("nb_patients", "patients distincts"))
    g.ajouter("ve_sejours", "Séjours hospitaliers", "mv_hospitalisations", kpi("nb_sejours", "séjours"))
    g.ajouter("ve_taux_hosp", "Taux d'hospitalisation", "mv_taux_hospitalisation",
              kpi("taux_hospitalisation", f"% des patients pris en charge ({periode_hosp})", [annees_hosp], fmt=",.2f"))
    g.ajouter("ve_deces", f"Décès {ANNEE_DECES}", "mv_deces", kpi("nb_deces", f"décès en France en {ANNEE_DECES}", [annee_deces]))
    g.ajouter("ve_satisf", f"Satisfaction {ANNEE_SATISFACTION}", "v_satisfaction",
              kpi("score_global", f"score e-Satis /100 ({ANNEE_SATISFACTION})", [annee_satisf], ",.1f"))
    g.ajouter("ve_courbe_consult", "Consultations par année", "mv_consultations",
              courbe(["nb_consultations", "nb_patients"]))
    g.ajouter("ve_courbe_hosp", "Taux d'hospitalisation par année (%)", "mv_taux_hospitalisation",
              courbe(["taux_hospitalisation"], fmt=",.2f", filtres=[annees_hosp]))
    g.ajouter("ve_carte_deces", f"Décès pour 100 000 habitants par région ({ANNEE_DECES})", "mv_deces",
              carte("taux_deces_100k", filtres=[annee_deces, region_connue]))
    g.ajouter("ve_carte_satisf", f"Score de satisfaction par région ({ANNEE_SATISFACTION})", "v_satisfaction",
              carte("score_global", couleurs="schemeBlues", filtres=[annee_satisf, region_connue], fmt=",.1f"))
    g.ajouter("ve_rattachement", "Consultations rattachées à un établissement", "mv_consultations",
              anneau("type_rattachement", "nb_consultations"))

    md_ve = bandeau(f"{nb(c['taux_hosp'], 2)} % des patients hospitalisés dans l'année · décès {ANNEE_DECES} : "
                    f"{top_n[0][0]} 1re en nombre, {top_t[0][0]} 1re pour 100 000 hab. · "
                    f"satisfaction {ANNEE_SATISFACTION} : {nb(c['satisf'], 1)}/100")
    md_limites = ("md", f"""<small>**Sources :** base des soins, référentiel FINESS, e-Satis (HAS), décès et populations légales 2019 (INSEE). **Limites :** {nb(pct_sans, 1)} % des consultations sans établissement · décès comptés au lieu du décès, fichier arrêté début {c['deces_annee_max'][0]} · hospitalisations {c['hosp_min']}-{c['hosp_max']} · taux de mortalité bruts (non standardisés par âge) · Mayotte : population 2017 · patients et professionnels pseudonymisés.</small>""")

    ve = Page("CHU - Vue d'ensemble", g)
    ve.grille([
        [(md_ve, 12, HAUTEUR_BANDEAU)],
        [("ve_consult", 2, 22), ("ve_patients", 2, 22), ("ve_sejours", 2, 22), ("ve_taux_hosp", 2, 22),
         ("ve_deces", 2, 22), ("ve_satisf", 2, 22)],
        [("ve_courbe_consult", 6, 50), ("ve_courbe_hosp", 6, 50)],
        [("ve_carte_deces", 4, 60), ("ve_carte_satisf", 4, 60), ("ve_rattachement", 4, 60)],
        [(md_limites, 12, 10)],
    ])
    filtres_ve = [
        filtre("ve_annee", "Année (activité de soins)", "mv_consultations", "annee", ve,
               exclure=["ve_deces", "ve_satisf", "ve_carte_deces", "ve_carte_satisf", "ve_courbe_consult",
                        "ve_courbe_hosp"],
               description="Filtre les indicateurs de consultation et d'hospitalisation."),
    ]

    # ---------------- Praticiens ----------------
    g.ajouter("pr_consult", "Consultations", "mv_consultations", kpi("nb_consultations", "consultations"))
    g.ajouter("pr_patients", "Patients", "mv_consultations", kpi("nb_patients", "patients distincts"))
    g.ajouter("pr_cpp", "Consultations par patient", "mv_consultations", kpi("consult_par_patient", "en moyenne", fmt=",.2f"))
    g.ajouter("pr_duree", "Durée moyenne", "mv_consultations", kpi("duree_moyenne", "minutes par consultation", fmt=",.1f"))
    g.ajouter("pr_chapitres", "B2 - Consultations par chapitre de diagnostic (CIM-10)", "mv_consultations",
              top(libelle_court("chapitre_diagnostic", 45), "nb_consultations", n=12))
    g.ajouter("pr_diag_top", "B2 - Taux de consultation : les 15 diagnostics les plus fréquents", "mv_consultations",
              top(libelle_court("diagnostic"), part("% des consultations"), fmt=",.3%"),
              "Part = consultations du diagnostic / consultations de la période filtrée. Liste complète : menu du graphique, « Voir les données ».")
    g.ajouter("pr_evol_chap", "Consultations par année et par sexe", "mv_consultations",
              barres("date_annee", ["nb_consultations"], horizontal=False, groupby=["sexe"], empile=True))
    g.ajouter("pr_specialite", "Consultations par spécialité médicale", "mv_consultations",
              barres("specialite", ["nb_consultations"], limite=12))
    g.ajouter("pr_motif", "Motifs de consultation", "mv_consultations",
              barres("motif", ["nb_consultations"], limite=10))
    g.ajouter("pr_age_consult", "Consultations par tranche d'âge et sexe", "mv_consultations",
              barres("tranche_age", ["nb_consultations"], horizontal=False, groupby=["sexe"], tri_asc=True,
                     tri="tranche_age"))

    g.ajouter("pr_sejours", "Séjours", "mv_hospitalisations", kpi("nb_sejours", "séjours hospitaliers"))
    g.ajouter("pr_patients_hosp", "Patients hospitalisés", "mv_hospitalisations", kpi("nb_patients_hosp", "patients distincts"))
    g.ajouter("pr_taux_hosp", "B3 - Taux d'hospitalisation", "mv_taux_hospitalisation",
              kpi("taux_hospitalisation", f"% des patients pris en charge ({periode_hosp})", [annees_hosp], fmt=",.2f"))
    g.ajouter("pr_dms", "Durée moyenne de séjour", "mv_hospitalisations", kpi("duree_moyenne_sejour", "jours", fmt=",.1f"))
    g.ajouter("pr_taux_annee", "B3 - Taux global d'hospitalisation par année (%)", "mv_taux_hospitalisation",
              courbe(["taux_hospitalisation"], fmt=",.2f", titre_y="%", filtres=[annees_hosp]))
    g.ajouter("pr_heatmap", "B4 - Séjours par chapitre de diagnostic et par année", "mv_hospitalisations",
              heatmap("annee", "chapitre_diagnostic", "nb_sejours"))
    g.ajouter("pr_pyramide", "B5 - Pyramide des âges des patients hospitalisés", "mv_hospitalisations",
              {**barres("tranche_age_10", ["sejours_femmes", "sejours_hommes_neg"], empile=True, tri="tranche_age_10",
                        tri_asc=True, legende=True, inverser=False,
                        filtres=[f_where("tranche_age_10 <> 'Inconnu'", "age_connu")]),
               "color_scheme": PALETTE_SEXE},
              "Hommes à gauche (valeurs négatives), femmes à droite.")
    g.ajouter("pr_taux_sexe_age", "B5 - Taux d'hospitalisation par sexe et tranche d'âge (%)", "mv_taux_hospitalisation",
              barres("tranche_age", ["taux_hospitalisation"], horizontal=False, groupby=["sexe"],
                     tri="tranche_age", tri_asc=True, fmt=",.2f",
                     filtres=[annees_hosp, f_where("sexe <> 'Indéterminé' AND tranche_age <> 'Inconnu'", "connus")]))
    g.ajouter("pr_diag_hosp", "B4 - Taux d'hospitalisation : les 15 diagnostics les plus fréquents", "mv_hospitalisations",
              top(libelle_court("diagnostic"), part("% des séjours"), fmt=",.2%"),
              "Part = séjours du diagnostic / séjours de la période filtrée. Liste complète : menu du graphique, « Voir les données ».")

    age_max = c["taux_hosp_age"][0] if c["taux_hosp_age"] else ("-", 0)
    age_min = c["taux_hosp_age"][-1] if c["taux_hosp_age"] else ("-", 0)
    md_pr1 = bandeau(f"1er recours : {chapitre_court(c['top_chap_consult'][0])} ({nb(c['top_chap_consult'][2], 1)} % des consultations) · "
                     f"{nb(c['motif_consult'], 1)} % de consultations simples")
    md_pr2 = bandeau(f"{nb(c['taux_hosp'], 2)} % des patients hospitalisés ({periode_hosp}) · 1er chapitre : "
                     f"{chapitre_court(c['top_chap_hosp'][0])} ({nb(c['top_chap_hosp'][2], 1)} %) · taux stable avec l'âge "
                     f"({nb(age_min[1], 2)} à {nb(age_max[1], 2)} %), à valider avec les praticiens")

    pr = Page("CHU - Praticiens", g)
    pr.onglet("Consultations et diagnostics", [
        [(md_pr1, 12, HAUTEUR_BANDEAU)],
        [("pr_consult", 3, 20), ("pr_patients", 3, 20), ("pr_cpp", 3, 20), ("pr_duree", 3, 20)],
        [("pr_chapitres", 6, 60), ("pr_diag_top", 6, 60)],
        [("pr_age_consult", 6, 50), ("pr_specialite", 6, 50)],
        [("pr_evol_chap", 6, 50), ("pr_motif", 6, 50)],
    ])
    pr.onglet("Hospitalisations", [
        [(md_pr2, 12, HAUTEUR_BANDEAU)],
        [("pr_sejours", 3, 20), ("pr_patients_hosp", 3, 20), ("pr_taux_hosp", 3, 20), ("pr_dms", 3, 20)],
        [("pr_taux_annee", 6, 50), ("pr_pyramide", 6, 50)],
        [("pr_taux_sexe_age", 6, 60), ("pr_diag_hosp", 6, 60)],
        [("pr_heatmap", 12, 70)],
    ])
    filtres_pr = [
        filtre("pr_annee", "Année", "mv_consultations", "annee", pr),
        filtre("pr_chapitre", "Chapitre de diagnostic", "mv_consultations", "chapitre_diagnostic", pr),
        filtre("pr_sexe", "Sexe", "mv_consultations", "sexe", pr),
        filtre("pr_age", "Tranche d'âge", "mv_consultations", "tranche_age", pr),
    ]

    # ---------------- Direction ----------------
    g.ajouter("di_consult", "Consultations", "mv_consultations", kpi("nb_consultations", "consultations"))
    g.ajouter("di_etab", "Établissements actifs", "mv_consultations", kpi("nb_etablissements", "avec au moins une consultation"))
    g.ajouter("di_rattach", "Taux de rattachement", "mv_consultations",
              kpi("taux_rattachement", "% des consultations avec un établissement connu", fmt=",.1f"))
    g.ajouter("di_sejours", "Séjours", "mv_hospitalisations", kpi("nb_sejours", "séjours hospitaliers"))
    g.ajouter("di_top_etab", "B1 - Taux de consultation : les 15 établissements les plus consultés", "mv_consultations",
              top(libelle_court("etablissement", 38), part("% des consultations rattachées"), filtres=[hors_inconnu],
                  fmt=",.2%"),
              "Part = consultations de l'établissement / consultations rattachées à un établissement sur la période "
              "filtrée. Liste complète : menu du graphique, « Voir les données ».")
    g.ajouter("di_rattachement", "Où ont lieu les consultations ?", "mv_consultations",
              anneau("type_rattachement", "nb_consultations"))
    g.ajouter("di_regions_consult", "Consultations par région de l'établissement", "mv_consultations",
              barres("region", ["nb_consultations"], limite=20, filtres=[hors_inconnu]))
    g.ajouter("di_b6", f"B6 - Les 15 professionnels les plus consultés (≥ {SEUIL_PATIENTS} patients)", "mv_consultations_pro",
              top({"expressionType": "SQL", "label": "Professionnel",
                   "sqlExpression": "code_professionnel || ' · ' || left(specialite, 22)"},
                  "nb_consultations",
                  filtres=[f_where("id_professionnel <> 'INCONNU'", "connu"),
                           f_having(f"COUNT(DISTINCT sk_patient) >= {SEUIL_PATIENTS}", "seuil")]),
              f"Nombre de consultations. Seuil de confidentialité : seuls les professionnels ayant vu au moins "
              f"{SEUIL_PATIENTS} patients distincts sont affichés ; professionnels pseudonymisés.")
    md_di1 = bandeau(f"{nb(pct_sans, 1)} % des consultations sans établissement : le taux B1 est un minimum · "
                     f"action : faire saisir le FINESS du lieu de consultation · B6 limité à "
                     f"{nb(c['pros_seuil'])} professionnels (≥ {SEUIL_PATIENTS} patients)")

    g.ajouter("di_kpi_deces", "Décès", "mv_deces", kpi("nb_deces", "décès (année filtrée)"))
    g.ajouter("di_kpi_age", "Âge moyen au décès", "mv_deces", kpi("age_moyen_deces", "ans", fmt=",.1f"))
    g.ajouter("di_kpi_taux", "Décès pour 100 000 habitants", "mv_deces",
              kpi({"expressionType": "SQL", "label": "Décès pour 100 000 hab.", "hasCustomLabel": True,
                   "optionName": "taux_france",
                   "sqlExpression": "100000.0 * SUM(nb_deces) / "
                                    "(SELECT SUM(population_2019) FROM gold.ref_population_region)"},
                  "France entière (population INSEE 2019)", fmt=",.0f"))
    g.ajouter("di_carte_nb", "B7 - Nombre de décès par région", "mv_deces",
              carte("nb_deces", filtres=[region_connue], couleurs="purples"))
    g.ajouter("di_carte_taux", "B7 - Décès pour 100 000 habitants par région", "mv_deces",
              carte("taux_deces_100k", filtres=[region_connue, pop_connue]))
    g.ajouter("di_rang_nb", "Classement des régions : nombre de décès", "mv_deces",
              barres("region", ["nb_deces"], limite=20, filtres=[region_connue]))
    g.ajouter("di_rang_taux", "Classement des régions : décès pour 100 000 habitants", "mv_deces",
              barres("region", ["taux_deces_100k"], limite=20, filtres=[region_connue, pop_connue], fmt=",.0f"))
    g.ajouter("di_pyramide_deces", "Pyramide des âges au décès", "mv_deces",
              {**barres("tranche_age_10", ["deces_femmes", "deces_hommes_neg"], empile=True, tri="tranche_age_10",
                        tri_asc=True, legende=True, inverser=False,
                        filtres=[f_where("tranche_age_10 <> 'Inconnu'", "age_connu")]),
               "color_scheme": PALETTE_SEXE}, "Hommes à gauche (valeurs négatives), femmes à droite.")
    g.ajouter("di_evol_deces", f"Décès par année en France (1990-{c['deces_annee_max'][0] - 1})", "mv_deces",
              courbe(["nb_deces"], filtres=[f_where(f"annee BETWEEN 1990 AND {c['deces_annee_max'][0] - 1}",
                                                    "annees_completes")]),
              "Avant 1990, le fichier INSEE des décès est incomplet (moins de 480 000 décès par an) ; l'année la "
              "plus récente du fichier est partielle. Les deux sont exclues.")
    g.ajouter("di_carte_dep", "Décès pour 100 000 habitants par département", "mv_deces",
              carte("taux_deces_dep_100k", entite="code_carte_departement", pays="france",
                    filtres=[f_where("code_carte_departement IS NOT NULL AND population_departement IS NOT NULL",
                                     "dep_connu")]),
              "Population légale INSEE 2019 du département (Mayotte : 2017).")
    g.ajouter("di_synthese_deces", "Synthèse régionale de la mortalité", "mv_deces",
              tableau(["region"], ["taux_deces_100k", "age_moyen_deces", "nb_deces", "population_region"],
                      filtres=[region_connue], tri="taux_deces_100k", page=20,
                      couleurs={"taux_deces_100k": "#E69F00", "age_moyen_deces": "#56B4E9"}),
              "Un âge moyen au décès élevé signale une population plus âgée : à lire avec le taux.")
    rang_txt = f"**{rang_idf}e sur {c['deces_nb_regions']}**" if rang_idf else "-"
    age_top = c["deces_age_region"].get(top_t[0][0]) or 0
    age_idf = c["deces_age_region"].get("Île-de-France") or 0
    md_di2 = bandeau(f"{ANNEE_DECES} : {top_n[0][0]} 1re en nombre ({nb(top_n[0][1])}) mais {rang_txt} pour 100 000 hab. · "
                     f"{top_t[0][0]} 1re ({nb(top_t[0][2])}) · âge moyen au décès {nb(age_top, 1)} contre "
                     f"{nb(age_idf, 1)} ans : comparer à âge égal")

    g.ajouter("di_kpi_score", "Score global", "v_satisfaction", kpi("score_global", "sur 100, pondéré par les répondants", fmt=",.1f"))
    g.ajouter("di_kpi_reco", "Recommandation", "v_satisfaction", kpi("taux_recommandation", "% recommanderaient l'établissement", fmt=",.1f"))
    g.ajouter("di_kpi_rep", "Répondants", "v_satisfaction", kpi("nb_reponses", "patients ayant répondu"))
    g.ajouter("di_kpi_etab_sat", "Établissements évalués", "v_satisfaction", kpi("nb_etablissements", "établissements"))
    g.ajouter("di_carte_sat", "B8 - Score de satisfaction par région", "v_satisfaction",
              carte("score_global", couleurs="schemeBlues", filtres=[region_connue], fmt=",.1f"))
    g.ajouter("di_rang_sat", "B8 - Classement des régions (score global sur 100)", "v_satisfaction",
              barres("region", ["score_global"], limite=20, fmt=",.1f", filtres=[region_connue]))
    g.ajouter("di_radar", "Profil de satisfaction par type d'enquête", "v_satisfaction",
              radar("enquete", [("Accueil", score("score_accueil")),
                                ("Prise en charge infirmière", score("score_pec_infirmier")),
                                ("Prise en charge médicale", score("score_pec_medical")),
                                ("Chambre", score("score_chambre")), ("Repas", score("score_repas")),
                                ("Organisation de la sortie", score("score_sortie"))]))
    g.ajouter("di_synthese_sat", "Synthèse régionale de la satisfaction", "v_satisfaction",
              tableau(["region"], ["score_global", "taux_recommandation", "score_accueil", "score_pec_medical",
                                   "score_repas", "score_sortie", "nb_reponses", "nb_etablissements"],
                      filtres=[region_connue], tri="score_global", page=20,
                      couleurs={m_: "#56B4E9" for m_ in ("score_global", "taux_recommandation", "score_accueil",
                                                         "score_pec_medical", "score_repas", "score_sortie")}))
    g.ajouter("di_etab_sat", "Les 15 établissements les moins bien notés (score sur 100)", "v_satisfaction",
              top(libelle_court("etablissement", 38), "score_global", fmt=",.1f", plus_faibles=True),
              "Score global pondéré par les répondants (toutes enquêtes, ou celle choisie dans le filtre). "
              "Liste complète : menu du graphique, « Voir les données ».")
    meilleur, moins_bon = (metro[0], metro[-1]) if metro else (("-", 0), ("-", 0))
    dom = [r for r in sat if r not in metro]
    dom_txt = (", outre-mer en dessous"
               if dom and metro and max(r[1] for r in dom) < min(r[1] for r in metro) else "")
    faible_txt = " et ".join(f"{lib} ({nb(v, 1)})" for lib, v in c["sous_scores"][:2]) or "-"
    faible_txt = faible_txt.replace("l'organisation de la sortie", "la sortie")
    md_di3 = bandeau(f"{ANNEE_SATISFACTION} : {nb(c['satisf'], 1)}/100 en France · de {meilleur[0]} ({nb(meilleur[1], 1)}) "
                     f"à {moins_bon[0]} ({nb(moins_bon[1], 1)}){dom_txt} · points faibles : {faible_txt}")

    g.ajouter("di_bench_barres", "Temps de réponse des requêtes métier : avant / après optimisation (ms, échelle log)",
              "bench_resultats", {**barres("requete", ["temps_ms"], horizontal=False, groupby=["configuration"],
                                           tri="requete", tri_asc=True, fmt=",.1f"), "logAxis": True,
                                  "show_value": True, "y_axis_bounds": [0.1, None]},
              "Médiane de 5 exécutions à chaud, mesurée par benchmark/mesurer.py.")
    g.ajouter("di_bench_table", "Détail des mesures", "bench_resultats",
              {"viz_type": "table", "query_mode": "raw",
               "all_columns": ["requete", "libelle", "configuration", "mediane_ms", "min_ms", "max_ms", "nb_lignes",
                               "date_mesure"],
               "order_by_cols": [json.dumps(["requete", True])], "row_limit": 1000, "server_pagination": False,
               "page_length": 30, "include_search": True, "adhoc_filters": [], "table_timestamp_format": "%d/%m/%Y %H:%M"})
    md_perf = bandeau("Avant = gold_v0 (ni partition ni index) · Après = gold (partitions annuelles + index) · "
                      "Vue BI = vues matérialisées gold.mv_* · " + resume_benchmark(c["bench"]))

    di = Page("CHU - Direction", g)
    di.onglet("Activité des établissements", [
        [(md_di1, 12, HAUTEUR_BANDEAU)],
        [("di_consult", 3, 20), ("di_etab", 3, 20), ("di_rattach", 3, 20), ("di_sejours", 3, 20)],
        [("di_top_etab", 6, 62), ("di_b6", 6, 62)],
        [("di_regions_consult", 7, 60), ("di_rattachement", 5, 60)],
    ])
    di.onglet("Mortalité", [
        [(md_di2, 12, HAUTEUR_BANDEAU)],
        [("di_kpi_deces", 4, 20), ("di_kpi_taux", 4, 20), ("di_kpi_age", 4, 20)],
        [("di_carte_nb", 6, 60), ("di_carte_taux", 6, 60)],
        [("di_rang_nb", 6, 55), ("di_rang_taux", 6, 55)],
        [("di_carte_dep", 6, 65), ("di_synthese_deces", 6, 65)],
        [("di_pyramide_deces", 6, 55), ("di_evol_deces", 6, 55)],
    ])
    di.onglet("Satisfaction", [
        [(md_di3, 12, HAUTEUR_BANDEAU)],
        [("di_kpi_score", 3, 20), ("di_kpi_reco", 3, 20), ("di_kpi_rep", 3, 20), ("di_kpi_etab_sat", 3, 20)],
        [("di_carte_sat", 6, 60), ("di_rang_sat", 6, 60)],
        [("di_radar", 5, 62), ("di_etab_sat", 7, 62)],
        [("di_synthese_sat", 12, 60)],
    ])
    di.onglet("Performance de la plateforme", [
        [(md_perf, 12, HAUTEUR_BANDEAU)],
        [("di_bench_barres", 12, 80)],
        [("di_bench_table", 12, 60)],
    ])
    filtres_di = [
        filtre("di_annee", "Année (activité)", "mv_consultations", "annee", di, onglets=["Activité des établissements"]),
        filtre("di_region", "Région", "mv_consultations", "region", di, onglets=["Activité des établissements"]),
        filtre("di_etab", "Établissement", "mv_consultations", "etablissement", di,
               onglets=["Activité des établissements"]),
        filtre("di_annee_deces", "Année des décès", "mv_deces", "annee", di, onglets=["Mortalité"],
               exclure=["di_evol_deces"], defaut=[ANNEE_DECES], multiple=False),
        filtre("di_sexe_deces", "Sexe", "mv_deces", "sexe", di, onglets=["Mortalité"]),
        filtre("di_annee_sat", "Année d'enquête", "v_satisfaction", "annee", di, onglets=["Satisfaction"],
               defaut=[ANNEE_SATISFACTION], multiple=False),
        filtre("di_enquete", "Type d'enquête", "v_satisfaction", "enquete", di, onglets=["Satisfaction"]),
    ]
    return g, (ve, filtres_ve), (pr, filtres_pr), (di, filtres_di)


def main():
    api = Api()
    bases = [b for b in api.tous("database") if b["database_name"] == NOM_BASE]
    if not bases:
        sys.exit(f"Connexion « {NOM_BASE} » introuvable : démarrer Superset (init_chu.sh la déclare).")
    base_id = bases[0]["id"]
    # SQL Lab : la connexion est visible et en lecture seule (aucun DML)
    api.put(f"database/{base_id}", {"expose_in_sqllab": True, "allow_dml": False, "allow_run_async": False})

    print("1. Chiffres clés (Gold)")
    c = chiffres_cles()
    print("2. Jeux de données")
    JEUX["bench_resultats"] = {"description": "Temps de réponse mesurés par benchmark/mesurer.py.", "metriques": [
        m("temps_ms", "MAX(mediane_ms)", "Temps médian (ms)", ",.1f")]}
    jeux = preparer_jeux(api, base_id)
    for table, ds in jeux.items():
        COLONNES[table] = {col["column_name"] for col in api.get(f"dataset/{ds}")["result"]["columns"]}
    print("3. Inventaire des graphiques et tableaux déjà construits (mis à jour en place)")
    existants, tableaux, doublons = inventaire(api)
    print("4. Rôles et comptes")
    tous_jeux = list(jeux)
    roles = roles_et_comptes([t for t in tous_jeux if t not in ("mv_consultations_pro", "bench_resultats")], tous_jeux)
    print("5. Graphiques et tableaux de bord")
    g, (ve, fve), (pr, fpr), (di, fdi) = construire(api, jeux, c, existants)
    publier(api, ve, "CHU - Vue d'ensemble", "chu-vue-ensemble", fve, [roles["Praticien"], roles["Direction"]], g, tableaux)
    publier(api, pr, "CHU - Praticiens", "chu-praticiens", fpr, [roles["Praticien"], roles["Direction"]], g, tableaux)
    publier(api, di, "CHU - Direction", "chu-direction", fdi, [roles["Direction"]], g, tableaux)
    n = nettoyer(api, existants, set(g.ids), doublons)
    print(f"  {len(set(g.ids) & set(existants))} graphiques mis à jour, {len(set(g.ids) - set(existants))} créés, "
          f"{n} obsolètes supprimés")
    print("6. Requêtes de démonstration (SQL Lab)")
    requetes_demo(api, base_id)
    print(f"Terminé : {len(g.ids)} graphiques.")


if __name__ == "__main__":
    main()
