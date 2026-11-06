"""Jeu de données Bronze FICTIF au format EXACT des données réelles du projet
(dump PostgreSQL DATA2023, CSV "Etablissement de SANTE", Hospitalisations.csv,
e-Satis CSV + xlsx, deces.csv), avec des cas sales volontaires. Sert aux tests
et à une démo locale :

    python tests/fixtures_bronze.py /tmp/bronze_demo

Les commentaires "-> ..." indiquent le résultat attendu en Silver.
"""
from __future__ import annotations

import os
import sys

D = "2026-11-01"   # rangement NiFi : bronze/<source>/<AAAA-MM-JJ>/<fichier>


def _w(path: str, text: str, encoding: str = "utf-8") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding=encoding, newline="\n") as f:
        f.write(text)


def manifest(path: str, source: str, sha256: str | None = None) -> None:
    """Manifeste NiFi à côté du fichier (mêmes attributs que le flux du Bronze)."""
    import hashlib
    import json
    digest = sha256 or hashlib.sha256(open(path, "rb").read()).hexdigest()
    with open(path + ".manifest.json", "w", encoding="utf-8") as f:
        json.dump({"source": source, "filename": os.path.basename(path), "content_SHA-256": digest,
                   "ingest.date": D}, f, indent=2)


def _parquet(spark, root, table, rows, schema):
    spark.createDataFrame(rows, schema).coalesce(1).write.mode("overwrite") \
        .parquet(f"{root}/s1_soins/{D}/{table}")


def _xlsx(path, header, rows):
    from openpyxl import Workbook
    os.makedirs(os.path.dirname(path), exist_ok=True)
    wb = Workbook()
    ws = wb.active
    ws.title = "Resultats"
    ws.append(header)
    for r in rows:
        ws.append(r)
    wb.save(path)


def build(spark, root: str) -> None:
    import datetime as dt

    # ------------------------------------------------------------------ COG
    _w(f"{root}/s0_cog/{D}/v_commune_2026.csv",
       "TYPECOM,COM,REG,DEP,CTCD,ARR,TNCC,NCC,NCCENR,LIBELLE,CAN,COMPARENT\n"
       "COM,59350,32,59,59D,594,0,LILLE,Lille,Lille,5999,\n"
       "COM,62041,32,62,62D,621,1,ARRAS,Arras,Arras,6299,\n"
       "COM,75056,11,75,75C,751,0,PARIS,Paris,Paris,7599,\n"
       "ARM,75101,,,,751,0,PARIS 1ER,Paris 1er,Paris 1er Arrondissement,,75056\n"  # -> dép/région de Paris
       "COM,2A004,94,2A,20D,2A1,0,AJACCIO,Ajaccio,Ajaccio,2A99,\n"
       "COM,01001,84,01,01D,012,5,ABERGEMENT,Abergement,L'Abergement-Clémenciat,0108,\n"
       "COMD,59355,,,,,0,LOMME,Lomme,Lomme,,59350\n"   # -> passage 59355 -> 59350
       "COMD,59350,,,,,0,LILLE,Lille,Lille,,59350\n"   # -> ignoré (même code que la commune)
       "COM,ABCDE,32,59,,,0,X,X,Commune invalide,,\n")  # -> rejet
    _w(f"{root}/s0_cog/{D}/v_departement_2026.csv",
       "DEP,REG,CHEFLIEU,TNCC,NCC,NCCENR,LIBELLE\n59,32,59350,2,NORD,Nord,Nord\n"
       "62,32,62041,2,PAS DE CALAIS,Pas-de-Calais,Pas-de-Calais\n75,11,75056,0,PARIS,Paris,Paris\n"
       "2A,94,2A004,3,CORSE DU SUD,Corse-du-Sud,Corse-du-Sud\n01,84,01053,5,AIN,Ain,Ain\n")
    _w(f"{root}/s0_cog/{D}/v_region_2026.csv",
       "REG,CHEFLIEU,TNCC,NCC,NCCENR,LIBELLE\n32,59350,4,HAUTS DE FRANCE,Hauts-de-France,Hauts-de-France\n"
       "11,75056,1,ILE DE FRANCE,Île-de-France,Île-de-France\n94,2A004,0,CORSE,Corse,Corse\n"
       "84,69123,1,AUVERGNE RHONE ALPES,Auvergne-Rhône-Alpes,Auvergne-Rhône-Alpes\n")
    _w(f"{root}/s0_cog/{D}/v_mvt_commune_2026.csv",
       "MOD,DATE_EFF,TYPECOM_AV,COM_AV,TNCC_AV,NCC_AV,NCCENR_AV,LIBELLE_AV,TYPECOM_AP,COM_AP,TNCC_AP,NCC_AP,NCCENR_AP,LIBELLE_AP\n"
       "31,2019-01-01,COM,62999,0,VIEUX,Vieux,Vieux,COM,62041,1,ARRAS,Arras,Arras\n")  # -> 62999 -> 62041

    # ------------------------- Source 2 : etablissement_sante.csv (format réel)
    head = ("adresse;cedex;code_commune;code_postal;commune;email;finess_etablissement_juridique;"
            "finess_site;identifiant_organisation;raison_sociale_site;siren_site;siret_site;telephone\n")
    _w(f"{root}/s2_etablissements/{D}/etablissement_sante.csv", head +
       "2 av Oscar Lambret 59000;;59350;59000;Lille;a@chu.fr;590000017;;F590780193;CHU DE LILLE;590780193;26590673500120;0320\n"
       "2 av Oscar Lambret 59000;;59350;59000;Lille;a@chu.fr;590000017;;F590780193;CHU DE LILLE;590780193;26590673500120;0320\n"  # -> fusionné
       "3 bd Besnier 62000;;62041;62000;Arras;;620000018;;F620100057;CH D'ARRAS;620100057;26620111100011;\n"
       ";;62041;62000;Arras;;;;F620100057;CH D'ARRAS;;;\n"            # -> conflit : version moins complète écartée
       "Route Saint-Antoine;;2A004;20000;Ajaccio;;2A0000005;;F2A0000013;CH AJACCIO;;;\n"   # -> FINESS 2A0000013
       "Rue X;;59355;59160;Lomme;;;;F05907801901012007;CLINIQUE LOMME;;12;\n"   # -> FINESS 059078019, commune 59350, SIRET NULL
       "Rue Y;;;62000;;;;;R10100000139221;CABINET DR X;;;\n"          # -> cabinet : FINESS NULL
       "Rue Z;;;;;;;12345;C12345678900011;FINESS FAUX;;;\n"           # -> rejet FINESS invalide
       "Rue W;;;;;;;;;SANS IDENTIFIANT;;;\n")                          # -> rejet id absent
    _w(f"{root}/s2_etablissements/{D}/activite_professionnel_sante.csv",
       "identifiant;identifiant_organisation;mode_exercice\n"
       "10001;F590780193;Salarié\n"
       "10002;C39806996300054;Libéral\n"      # -> écarté : un établissement F est prioritaire
       "10002;F620100057;Salarié\n"
       "10003;R10100000139221;Libéral\n"       # -> cabinet : id_organisation sans FINESS
       "10004;;Libéral\n")

    # ------------------------------------- Source 1 : base PostgreSQL (format réel)
    _parquet(spark, root, "professionnel_de_sante", [
        ("10001", "Monsieur", "Civil", "NOM", "Prenom", "Medecin", "RPPS", "PNE01"),
        ("10002", "Madame", "Civil", "NOM", "Prenom", "Medecin", "ADELI", "CAR01"),
        ("10003", "Monsieur", "Civil", "NOM", "Prenom", "Infirmier", "RPPS", None),
        (None, "Monsieur", "Civil", "NOM", "Prenom", "Medecin", "RPPS", None),   # -> rejet
    ], "Identifiant string, Civilite string, Categorie_professionnelle string, Nom string, Prenom string, "
       "Profession string, Type_identifiant string, Code_specialite string")
    _parquet(spark, root, "specialites", [("PNE01", "Medecin", "Pneumologie"), ("CAR01", "Medecin", "Cardiologie")],
             "Code_specialite string, Fonction string, Specialite string")
    _parquet(spark, root, "diagnostic", [
        ("J459", "Asthme, sans précision"), ("I10", "Hypertension essentielle"),
        ("I10", "Hypertension essentielle"), ("C50", "Tumeur maligne du sein"),
        ("XX", "Code faux"),  # -> rejet
    ], "Code_diag string, Diagnostic string")
    _parquet(spark, root, "patient", [
        # Date au format américain M/d/yyyy, comme dans la vraie base
        (1, "Martin", "Alice", "female", "5/10/1980", "59000", "alice@x.fr", "1800559000000"),  # 10 mai 1980
        (2, "Durand", "Paul", "male", "2/1/1950", "20100", None, None),     # 1er février 1950 ; -> 2A
        (3, "Petit", "Léa", "female", "1/1/2010", "97400", None, None),     # -> 974 absent du COG -> NULL
        (4, "Roux", "Marc", "male", "abc", "01000", None, None),            # -> année NULL, dép 01
        (None, "Sans", "Id", "female", "1/1/1990", "59000", None, None),    # -> rejet
    ], "Id_patient int, Nom string, Prenom string, Sexe string, Date string, Code_postal string, "
       "EMail string, Num_Secu string")
    d = dt.date
    _parquet(spark, root, "consultation", [
        (1, 1, "10001", "J459", d(2020, 3, 15), "09:00:00", "09:30:00", "Consultation"),
        (2, 2, "10002", "I10", d(2020, 6, 1), "20:00:00", "02:00:00", "Soins"),        # -> passage de minuit : 360 min
        (3, 3, "10003", "Z99", d(2021, 1, 10), "10:00:00", "10:00:00", "Bilan"),      # -> diag NULL, FINESS NULL, durée NULL
        (4, 99, "10001", "I10", d(2020, 1, 1), "08:00:00", "08:15:00", "X"),          # -> rejet patient inexistant
        (5, 1, "10001", "I10", None, "08:00:00", "08:15:00", "X"),                    # -> rejet date
        (7, 1, "10001", "J459", d(2020, 3, 15), "09:05:00", "09:30:00", "Consultation"),  # -> double saisie de 1
        (8, 2, "10002", "I10", d(2020, 7, 1), "08:00:00", "08:15:00", "A"),
        (8, 2, "10002", "I10", d(2020, 7, 1), "08:00:00", "08:15:00", "B"),           # -> conflit : 8 rejeté deux fois
        (9, 1, "10001", "I10", d(2099, 1, 1), "08:00:00", "08:15:00", "X"),           # -> rejet date future
        (10, 4, "55555", "C50", d(2019, 12, 31), "11:00:00", "11:45:00", "Annonce"),  # -> professionnel NULL
    ], "Num_consultation int, Id_patient int, Id_prof_sante string, Code_diag string, Date date, "
       "Heure_debut string, Heure_fin string, Motif string")
    _parquet(spark, root, "prescription", [(1, "A"), (1, "B"), (2, "C")], "Num_consultation int, Code_CIS string")

    # ------------------------------- Hospitalisations.csv (format réel, JJ/MM/AAAA)
    _w(f"{root}/s1_hospitalisations/{D}/Hospitalisations.csv",
       "Num_Hospitalisation;Id_patient;identifiant_organisation;Code_diagnostic;"
       "Suite_diagnostic_consultation;Date_Entree;Jour_Hospitalisation\n"
       "100;1;F590780193;J459;Asthme;01/04/2020;5\n"                # -> sortie 2020-04-06
       "101;2;F05907801901012007;I10;HTA;01/05/2020;2\n"           # -> FINESS 059078019
       "102;1;F590780193;I10;HTA;10/05/2020;-3\n"                  # -> rejet durée négative
       "103;1;R10100000139221;I10;HTA;10/05/2020;2\n"              # -> conservé : cabinet R...
       "106;2;F999999999;I10;HTA;12/05/2020;3\n"                  # -> conservé, organisation inconnue -> NULL
       "104;1;F590780193;I10;HTA;10/05/2020;\n"                    # -> rejet durée inconnue
       "105;77;F590780193;I10;HTA;10/05/2020;1\n")                 # -> rejet patient inexistant

    manifest(f"{root}/s1_hospitalisations/{D}/Hospitalisations.csv", "s1_hospitalisations")

    # ------------------------------------------- Source 3 : e-Satis (format réel)
    # finess = juridique ; finess_geo = site. CSV 2019 en Windows-1252, décimales mixtes.
    h48 = ("finess;rs_finess;finess_geo;rs_finess_geo;region;participation;Depot;nb_rep_score_all_rea_ajust;"
           "score_all_rea_ajust;classement;evolution;nb_rep_score_accueil_rea_ajust;score_accueil_rea_ajust;"
           "nb_rep_score_PECinf_rea_ajust;score_PECinf_rea_ajust;nb_rep_score_PECmed_rea_ajust;score_PECmed_rea_ajust;"
           "nb_rep_score_chambre_rea_ajust;score_chambre_rea_ajust;nb_rep_score_repas_rea_ajust;score_repas_rea_ajust;"
           "nb_rep_score_sortie_rea_ajust;score_sortie_rea_ajust;taux_reco_brut;nb_reco_brut\n")
    row = "{j};RS;{g};RS GEO;Hauts-de-France;1- Obligatoire;1- Oui;300;{s};{c};2-Stable;300;80.1;300;78;300;79,2;300;70;300;60,5;300;65;90,1;300\n"
    _w(f"{root}/s3_satisfaction/{D}/resultats-esatis48h-mco-open-data-2019.csv", h48
       + row.format(j="590000017", g="590780193", s="75,5", c="A")
       + row.format(j="620000018", g="620100057", s="120", c="B")    # -> rejet score > 100
       + row.format(j="2A0000005", g="2A0000013", s="70", c="Z")     # -> rejet classement
       + row.format(j="999999990", g="999999999", s="70", c="B")     # -> hors périmètre
       + row.format(j="590000017", g="59078019", s="", c="DI")       # -> conservé, FINESS 059078019
       + row.format(j="620000018", g="620100057", s="", c="A"),      # -> rejet score global vide + A
       encoding="cp1252")
    _w(f"{root}/s3_satisfaction/{D}/lexique-esatis48h-mco-open-data-2019.csv",
       "Nom de la variable;Libellé\nfinness;x\n", encoding="cp1252")              # -> ignoré (lexique)
    _w(f"{root}/s3_satisfaction/{D}/resultats-iqss-open-data-2019.csv", "a;b\n1;2\n")  # -> ignoré (pas e-Satis)
    # 2020 : e-Satis CA en .xlsx, FINESS en cellule numérique (zéro de tête perdu)
    _xlsx(f"{root}/s3_satisfaction/{D}/resultats-esatisca-mco-open-data-2020.xlsx",
          ["finess", "rs_finess", "finess_geo", "rs_finess_geo", "region", "participation", "Depot",
           "nb_rep_score_all_ajust", "score_all_ajust", "classement", "evolution", "score_AVH_ajust",
           "nb_rep_score_AVH_ajust", "score_ACC_ajust", "nb_rep_score_ACC_ajust", "score_PEC_ajust",
           "nb_rep_score_PEC_ajust", "score_CER_ajust", "nb_rep_score_CER_ajust", "score_OVS_ajust",
           "nb_rep_score_OVS_ajust", "taux_reco_brut", "nb_reco_brut"],
          [["590000017", "RS", 590780193, "RS GEO", "Hauts-de-France", "1- Obligatoire", "1- Oui",
            523, 76.56, "B", None, 81.88, 523, 79.53, 523, 84.3, 523, 69.29, 523, 68.27, 523, 66.7, 522],
           ["590000017", "RS", 59078019, "RS GEO", "Hauts-de-France", "1- Obligatoire", "1- Oui",
            100, 80.0, "A", None, 81, 100, 82, 100, 83, 100, 84, 100, 85, 100, 86, 100]])

    # --------------------------------------------- Source 4 : deces.csv (format réel)
    _w(f"{root}/s4_deces/{D}/deces.csv",
       "nom,prenom,sexe,date_naissance,code_lieu_naissance,lieu_naissance,pays_naissance,"
       "date_deces,code_lieu_deces,numero_acte_deces\n"
       "DUPONT,JEAN,1,19400101,59350,LILLE,,20190315,59350,001\n"
       "MARTIN,ANNE,2,19300000,59350,LILLE,,00/00/2019,59355,002\n"     # -> partielle, 59355 -> 59350
       "DURAND,LUC,1,19500101,59350,LILLE,,2019-06-01,99100,003\n"     # -> hors périmètre (étranger)
       "DUPONT,JEAN,1,19400101,59350,LILLE,,20190315,59350,001\n"       # -> doublon strict
       "PETIT,EVE,2,19800101,59350,LILLE,,20191301,59350,005\n"         # -> rejet date invalide
       "ROUX,PAUL,1,20200101,59350,LILLE,,20190101,59350,006\n"         # -> rejet naissance > décès
       "LEROY,ALICE,2,1945-01-01,75056,PARIS,,10/07/2019,75101,007\n"   # -> arrondissement conservé
       "MOREL,MARC,1,19450101,75056,PARIS,,20190710,12345,008\n"        # -> commune inconnue -> NULL
       "FAURE,LEA,2,19500615,01001,AIN,,20190820,1001,009\n")           # -> code 1001 (zéro perdu) -> 01001


if __name__ == "__main__":
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
    from silver.common import get_spark
    build(get_spark("fixtures"), sys.argv[1] if len(sys.argv) > 1 else "/tmp/bronze_demo")
