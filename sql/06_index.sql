-- Index de la version optimisée (schéma gold uniquement, gold_v0 n'en a pas).
-- PostgreSQL n'indexe pas les clés étrangères automatiquement.
-- Index composites : colonne filtrée par égalité d'abord, date ensuite.
-- Un index créé sur une table partitionnée est créé sur chaque partition.

-- fait_consultation
CREATE INDEX ix_consult_etab_date ON gold.fait_consultation (sk_etablissement, sk_date);  -- B1
CREATE INDEX ix_consult_diag_date ON gold.fait_consultation (sk_diagnostic, sk_date);     -- B2
CREATE INDEX ix_consult_prof_date ON gold.fait_consultation (sk_professionnel, sk_date);  -- B6
CREATE INDEX ix_consult_patient   ON gold.fait_consultation (sk_patient);                 -- B3

-- fait_hospitalisation
CREATE INDEX ix_hospit_date_entree ON gold.fait_hospitalisation (sk_date_entree);                 -- B3, B5
CREATE INDEX ix_hospit_diag_date   ON gold.fait_hospitalisation (sk_diagnostic, sk_date_entree);  -- B4
CREATE INDEX ix_hospit_etab_date   ON gold.fait_hospitalisation (sk_etablissement, sk_date_entree);
CREATE INDEX ix_hospit_patient     ON gold.fait_hospitalisation (sk_patient);                     -- B5
CREATE INDEX ix_hospit_date_sortie ON gold.fait_hospitalisation (sk_date_sortie);

-- fait_satisfaction (sk_etablissement est déjà couvert par uq_satisfaction)
CREATE INDEX ix_satisf_date ON gold.fait_satisfaction (sk_date);  -- B8

-- fait_deces (pour B7 c'est surtout le partitionnement qui joue)
CREATE INDEX ix_deces_geo ON gold.fait_deces (sk_geo_deces);      -- B7

-- dimensions
CREATE INDEX ix_geo_region       ON gold.dim_geographie (code_region);                           -- B7, B8
CREATE INDEX ix_etab_finess      ON gold.dim_etablissement (finess_site, date_debut_validite);   -- B1
CREATE INDEX ix_etab_geo         ON gold.dim_etablissement (sk_geo);                             -- B8
CREATE INDEX ix_patient_geo      ON gold.dim_patient (sk_geo);
CREATE INDEX ix_temps_annee_mois ON gold.dim_temps (annee, mois);                                -- B8
