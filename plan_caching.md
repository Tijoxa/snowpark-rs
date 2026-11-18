# Plan de caching — Snowpark RS

## Objectif

Ajouter un cache Parquet partitionné à Snowpark RS, avec une API Python simple et une exécution hybride Snowflake / DataFusion.

## 1. API Python

```python
session = Session(
    connection=snowflake_connection,
    cache_dir="./cache",
    ttl=3600,
)

df = (
    DataFrame.table("DB.PROD.PRICES")
    .filter(col("ZONE") == "FR")
)

result = df.to_arrow(
    session,
    partition_by=["ZONE", "YEAR"],
)
```

- La `Session` centralise la connexion Snowflake, le répertoire de cache et le TTL.
- `partition_by` définit explicitement les colonnes de partitionnement Hive.
- Sans `partition_by`, `to_arrow(session)` exécute normalement la requête sur Snowflake, sans cache.
- Aucun partitionnement automatique n'est nécessaire.

## 2. Architecture

```text
Python DataFrame API
        ↓
snowpark-ast (Rust)
        ↓
to_arrow(session, partition_by)
        ↓
Session / Cache Engine (Rust)
   ├── Validation des colonnes
   ├── Cache Parquet + TTL
   ├── Snowflake (cache miss)
   └── DataFusion (cache hit)
        ↓
Apache Arrow
```

L'AST reste indépendant de la session et des mécanismes de caching.

## 3. Fonctionnement du cache

1. Valider que les colonnes de `partition_by` existent dans le schéma du DataFrame.
2. Lever une erreur explicite si une colonne est absente.
3. Identifier le dataset à partir de l'AST normalisé.
4. Vérifier les partitions Parquet disponibles et leur TTL.
5. Lire les partitions valides avec DataFusion.
6. Récupérer les données manquantes ou expirées depuis Snowflake.
7. Écrire les partitions avec un overwrite sélectif.
8. Retourner les données au format Arrow.

Exemple de stockage :

```text
cache/
  dataset_hash/
    ZONE=FR/
      YEAR=2025/
        part-000.parquet
      YEAR=2026/
        part-000.parquet
```

## 4. Gestion des partitions

- Utiliser le partitionnement Hive de Parquet.
- Gérer le TTL individuellement par partition.
- Permettre l'overwrite des partitions expirées.
- Éviter de recharger les partitions encore valides.
- Conserver des métadonnées indiquant la complétude et la date de rafraîchissement de chaque partition.

Le cache doit éviter de considérer un résultat partiellement filtré comme une partition complète.

## 5. Responsabilités

| Composant | Responsabilité |
|---|---|
| `snowpark-ast` | AST, génération SQL, identification des dépendances |
| API Python | DataFrame, `to_arrow`, paramètres utilisateur |
| Session Rust | Configuration, orchestration, TTL |
| Snowflake | Exécution distante et récupération des données |
| DataFusion | Lecture et exécution locale sur Parquet |
| Cache Engine | Index, partitions, invalidation, overwrite |

## 6. Plan d'implémentation

- [ ] Ajouter `cache_dir` et `ttl` à la Session.
- [ ] Ajouter `partition_by` à `to_arrow`.
- [ ] Valider les colonnes de partitionnement.
- [ ] Générer un identifiant stable depuis l'AST.
- [ ] Implémenter l'écriture Parquet avec Hive partitioning.
- [ ] Ajouter un manifeste des partitions et de leur TTL.
- [ ] Lire les partitions en cache avec DataFusion.
- [ ] Implémenter le rafraîchissement et l'overwrite sélectifs.
- [ ] Tester les cache hits, misses, expirations et partitions incomplètes.

## 7. Points à trancher

- Comment déterminer les partitions nécessaires à partir des filtres de l'AST ?
- Comment garantir qu'une partition est complète ?
- Comment gérer les requêtes dont les résultats ne peuvent pas être rafraîchis partition par partition ?
- Comment invalider le cache si les données Snowflake changent avant expiration du TTL ?
- Faut-il prévoir un cache non partitionné à terme ?

## Principe directeur

**L'utilisateur choisit le partitionnement ; la Session gère le caching ; l'AST décrit la requête ; Snowflake et DataFusion assurent l'exécution.**

La première version doit privilégier la simplicité et la correction des résultats plutôt qu'un optimiseur de cache entièrement automatique.
