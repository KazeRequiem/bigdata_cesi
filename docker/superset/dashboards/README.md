# Tableaux de bord Superset (Livrable 3)

Trois tableaux de bord construits par script à partir de la couche Gold, pour les deux profils de l'énoncé.

| Tableau (URL) | Rôles | Contenu |
|---|---|---|
| **CHU - Vue d'ensemble** `/superset/dashboard/chu-vue-ensemble/` | Praticien, Direction | 6 indicateurs clés, tendances, cartes mortalité 2019 et satisfaction 2020, couverture des données, limites |
| **CHU - Praticiens** `/superset/dashboard/chu-praticiens/` | Praticien, Direction | Onglet *Consultations et diagnostics* (B2) ; onglet *Hospitalisations* (B3, B4, B5 : taux par année, heatmap diagnostic × année, pyramide des âges, taux par sexe et âge) |
| **CHU - Direction** `/superset/dashboard/chu-direction/` | Direction | *Activité des établissements* (B1, B6 avec seuil de 10 patients) ; *Mortalité* (B7 : cartes en nombre et pour 100 000 hab., classements, carte des départements pour 100 000 hab., tableau de synthèse régional, pyramide, tendance 1970-2019) ; *Satisfaction* (B8 : carte, classement, radar des sous-scores, établissements les plus faibles, tableau de synthèse régional) ; *Performance de la plateforme* (temps de réponse avant / après optimisation) |

Chaque onglet commence par un **bandeau d'une ligne « À retenir »** dont les chiffres sont **calculés dans Gold au moment de la construction** (relancer le script après un rechargement des données) ; le récit détaillé est porté à l'oral.

Choix de rendu : pas de longs tableaux paginés. Les listes (diagnostics, établissements, professionnels, établissements les moins bien notés) sont des **Top 15 en barres horizontales**, valeur au bout de la barre (parts en %, calculées sur toute la période filtrée par une fonction de fenêtre `COUNT(*) / SUM(COUNT(*)) OVER ()`) ; la liste complète reste accessible par le menu du graphique (« Voir les données », export CSV). Les deux **tableaux de synthèse régionaux** gardent le format tableau, colorés par valeur dans chaque colonne (mise en forme conditionnelle).

## Construire ou reconstruire

Prérequis : Gold chargé (`sql/08` à `10`), Superset démarré.

```bash
# 1. vues dédiées aux tableaux de bord (une fois, ou si leur définition change ; ~2 min 30)
docker compose exec postgres psql -U chu -d chu_dw -f /sql/13_vues_dashboard.sql
# 2. temps de réponse pour l'onglet Performance (~1 min ; ne rien lancer d'autre pendant la mesure)
docker compose exec superset python /app/benchmark/mesurer.py
# 3. jeux de données, graphiques, tableaux, rôles, comptes de démo, requêtes SQL Lab (~30 s)
docker compose exec superset python /app/dashboards/construire_dashboards.py
```

Le script est relançable : il **met à jour en place** ses jeux de données, ses graphiques (description
commençant par `[L3:`) et ses tableaux (slug `chu-*`) ; identifiants et URL ne changent pas, un onglet resté
ouvert continue de fonctionner. Seuls les graphiques qu'il n'utilise plus et ses requêtes `Démo *` sont
supprimés. Une modification faite à la main dans l'interface sur ces objets est écrasée au prochain
lancement : la faire dans le script.

## Utiliser les filtres

- Choisir les valeurs **dans la liste**, puis cliquer **« Appliquer »** (à droite de la barre de filtres) : Superset 4.1 n'applique pas un filtre natif tant qu'on ne valide pas. Les graphiques se mettent à jour en ~1 s, sans recharger la page.
- Ne pas taper une valeur puis Entrée : la liste accepte une saisie libre (« 1999 » devient une valeur qui ne renvoie rien). L'option qui l'empêcherait (`searchAllOptions`) est remise à `False` par l'interface à chaque ouverture du tableau (elle réenregistre le tableau pour mémoriser les couleurs).
- Superset tourne avec **3 processus** (`SERVER_WORKER_AMOUNT`, variable `SUPERSET_WORKERS` du `.env`, 3 par défaut) : les ~10 graphiques d'un onglet sont calculés en parallèle (1 s au lieu de 3,6 s avec un seul processus).

Après chaque chargement des faits, les vues matérialisées sont rafraîchies par la tâche
`gold_vues_dashboard` du DAG Airflow (ou à la main : `sql/14_rafraichir_vues_dashboard.sql`, ~1 min 30),
puis relancer l'étape 3 pour mettre à jour les textes.

## Ce que le script crée

- **Jeux de données** (schéma `gold`) avec leurs **métriques SQL définies une seule fois** (couche sémantique) :

  | Jeu | Lignes | Sert à |
  |---|---:|---|
  | `mv_consultations` | 1,03 M | B1, B2 (sans identifiant de professionnel) |
  | `mv_consultations_pro` | 1,03 M | B6, **rôle Direction seulement** |
  | `mv_hospitalisations` | 2 479 | B4, B5 |
  | `mv_taux_hospitalisation` | 216 | B3, B5 (patients hospitalisés / pris en charge) |
  | `mv_deces` | ~125 000 | B7 (agrégat de 24,7 M de décès + population INSEE) |
  | `v_satisfaction` | 3 861 | B8 |
  | `bench_resultats` | 24 | onglet Performance |

- **Filtres natifs** (barre horizontale) : Vue d'ensemble = année ; Praticiens = année, chapitre CIM-10, sexe, tranche d'âge ; Direction = filtres propres à chaque onglet (année d'activité, région, établissement / année des décès, **2019 par défaut**, sexe / année d'enquête, **2020 par défaut**, type d'enquête).
- **Rôles** `Praticien` et `Direction` (droits de Gamma + accès aux jeux de données ; `mv_consultations_pro` et `bench_resultats` réservés à Direction) et droits par tableau (`DASHBOARD_RBAC`).
- **Comptes de démonstration** `praticien_demo` et `direction_demo` si `SUPERSET_DEMO_PASSWORD` est défini dans `.env`.
- **Requêtes SQL Lab** « Démo 1 » à « Démo 5 » pour la démonstration live :

  | Requête | Ce qu'elle montre | Temps observé (via SQL Lab) |
  |---|---|---|
  | Démo 1 | B7 sur la vue jointe `gold.v_deces_region` (refait la jointure sur 24,7 M de lignes) | 4 à 16 s |
  | Démo 2 | B7 sur `fait_deces` filtré sur la clé de partition | ~1 s |
  | Démo 3 | B7 sur `gold.mv_deces`, avec le taux pour 100 000 hab. | < 0,5 s |
  | Démo 4 | `EXPLAIN ANALYZE` : seule la partition `fait_deces_2019` est lue | < 0,5 s |
  | Démo 5 | B8 satisfaction par région 2020 | < 0,5 s |

## Choix techniques et pièges connus

- **Vues matérialisées** plutôt que les vues `gold.v_*` : un tableau lance 6 à 10 requêtes à la fois ; B7 passe de ~1 s (faits partitionnés) ou 4 s (vue jointe) à **1,5 ms**. Coût : 622 Mo et ~1 min 30 de rafraîchissement par chargement.
- **Cache** des résultats dans la base de métadonnées (`SupersetMetastoreCache`, 24 h) : pas de Redis dans la stack. Bouton « Forcer l'actualisation » pour recalculer.
- **Carte des régions** : le fond `france_regions` de Superset 4.1 n'utilise pas les codes ISO actuels pour 6 régions (Corse `FR-COR`, outre-mer `FR-GUA`, `FR-MTQ`, `FR-GUF`, `FR-LRE`, `FR-MAY`) et code l'Île-de-France `FR-IDF` **suivi d'une tabulation**. D'où la colonne `gold.ref_region_iso.code_carte` (avec la tabulation) utilisée par les cartes ; `code_iso` reste le vrai code ISO.
- **Pas de palette au niveau du tableau de bord** (`color_scheme` dans `json_metadata`) : elle s'impose aux cartes, qui passent alors en couleurs par catégorie au lieu d'un dégradé.
- **Taux pour 100 000 habitants** : populations légales INSEE 2019 par région (`gold.ref_population_region`) et par département (`gold.ref_population_departement`), sources dans `sql/13` ; contrôle : chaque région = somme de ses départements. Mayotte, absente du millésime 2019, prend sa population de référence 2017 (256 518). Métriques `taux_deces_100k` (regrouper **par région**) et `taux_deces_dep_100k` (**par département**). Un petit département avec un grand hôpital a un taux gonflé (décès comptés au lieu du décès), ex. Territoire de Belfort.
- **Pyramides des âges** : palette dédiée `chu_femmes_hommes` déclarée dans `superset_config.py` (`EXTRA_CATEGORICAL_COLOR_SCHEMES`) ; les couleurs par libellé du tableau de bord ne s'appliquent pas aux séries issues de métriques.
- **Radar** : métriques ad hoc avec libellé (sinon les axes affichent les noms techniques).
- **Hospitalisations** : la source n'a aucun séjour après 2021 ; les taux sont filtrés sur 2015-2021 pour ne pas afficher de faux 0 %.
- **Décès** : comptés au lieu du décès ; le fichier s'arrête début 2020 (année exclue de la tendance).
- **Professionnels** : déjà pseudonymisés en Silver ; le tableau B6 affiche un code court `PS-` + 12 caractères du pseudonyme et masque les professionnels sous 10 patients distincts.
- Sous **Git Bash**, préfixer les commandes `docker exec ... /sql/...` par `MSYS_NO_PATHCONV=1` (sinon le chemin est réécrit en `C:/Program Files/Git/sql/...`).
