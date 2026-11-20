# Plan de caching — Snowpark RS

## Objectif

Ajouter un cache Parquet partitionné à Snowpark RS, avec l'API paresseuse `Table` et les expressions Polars, une session explicite et une exécution hybride Snowflake / DataFusion. Le cache est optionnel et ne modifie pas la compilation hors ligne vers SQL.

## 1. API Python

API cible (les options de cache restent à implémenter) :

```python
import polars as pl
import snowpark_rs as pls

session = (
    pls.Session.builder
    .config("connection_name", "myconnection")
    .config("cache_dir", "./cache")
    .config("ttl", 3600)
    .create()
)

df = (
    pls.Table("DB.PROD.PRICES")
    .filter(pl.col("ZONE") == "FR")
)

sql, binds = df.to_sql()  # compilation hors ligne, sans cache
arrow = df.to_arrow(session, partition_by=["ZONE", "YEAR"])
data = df.to_polars(session, partition_by=["ZONE", "YEAR"])
```

- Le constructeur public `Table(name)` et les transformations composant des `pl.Expr` restent indépendants de la session ; le cache n'intervient qu'à l'exécution.
- `Session.builder` conserve la configuration actuelle du profil `connection_name` et accepte deux nouvelles options facultatives : `cache_dir` et `ttl` (durée en secondes, entière et positive).
- `partition_by` est un argument nommé facultatif de `to_arrow` et `to_polars` qui définit explicitement les colonnes de partitionnement Hive du résultat, avec la casse exacte des noms de sortie. Vérifier aussi que la session dispose d'un répertoire et d'un TTL de cache valides lorsque cette option est utilisée.
- Sans `partition_by`, `to_arrow(session)` et `to_polars(session)` exécutent normalement la requête sur Snowflake, sans cache.
- `to_polars(session, partition_by=...)` transmet l'option à `to_arrow` et conserve la conversion `pl.from_arrow` utilisée actuellement. La prise en charge du cache pour `to_arrow_batches(session)` et `to_polars(session, use_batches=True)` est à planifier séparément ; rejeter explicitement la combinaison `partition_by` / `use_batches=True` dans la première version.
- Aucun partitionnement automatique n'est nécessaire.

## 2. Architecture

```text
Table / DataFrame Python (pl.Expr)
        ↓
Traducteur Polars → snowflake-ast (Rust)
        ↓
to_arrow(session, partition_by=...) / to_polars(session, partition_by=...)
        ↓
Session / Cache Engine (Rust)
   ├── Validation des colonnes
   ├── Cache Parquet + TTL
   ├── Snowflake (cache miss)
   └── DataFusion (cache hit)
        ↓
Apache Arrow
        ↓ (pour to_polars uniquement)
polars.DataFrame
```

L'AST reste indépendant de la session et des mécanismes de caching. Le moteur de cache s'intègre en aval du traducteur partagé Rust/Python. DataFusion reste un moteur d'exécution interne ; il ne remplace pas Polars dans l'API publique. Conserver le cache dans le backend de session optionnel, sans ajouter de dépendances de cache à la compilation hors ligne de `snowflake-ast`.

## 3. Fonctionnement du cache

1. Valider que les colonnes de `partition_by` existent dans le schéma de sortie du DataFrame (`schema` / `columns` si connus, métadonnées de session ou schéma du manifeste sinon).
2. Lever une erreur explicite si une colonne est absente.
3. Identifier le dataset à partir du plan traduit normalisé, des paramètres typés dans leur ordre, des colonnes de partitionnement et du contexte Snowflake (compte, utilisateur, rôle, base et schéma). Résoudre le nom pleinement qualifié de la table source ; limiter le prototype aux plans dont une seule table source peut être identifiée sans ambiguïté.
4. Vérifier les partitions Parquet disponibles et leur TTL.
5. Lire les partitions valides avec DataFusion.
6. Récupérer le résultat depuis Snowflake si le cache est manquant ou expiré. Ne rafraîchir uniquement les partitions manquantes ou expirées que si le plan permet leur recalcul indépendant.
7. Écrire les partitions avec un overwrite sélectif lorsque possible, sinon reconstruire intégralement le dataset.
8. Retourner les données au format Arrow.

Exemple de stockage :

```text
cache/
  DB.PROD.PRICES/
    dataset_hash/
      manifest.json
      ZONE=FR/
        YEAR=2025/
          part-000.parquet
        YEAR=2026/
          part-000.parquet
```

Le répertoire `cache_dir / "DB.PROD.PRICES"` regroupe les datasets de la table, avec un identifiant de plan et de contexte distinct sous ce répertoire. Deux requêtes utilisant des filtres, paramètres ou projections différents ne doivent pas partager un cache de résultats par erreur. Les noms de répertoires doivent être encodés de manière sûre sur les systèmes de fichiers pris en charge.

## 4. Gestion des partitions

- Utiliser le partitionnement Hive de Parquet.
- Gérer le TTL individuellement par partition.
- Permettre l'overwrite des partitions expirées.
- Éviter de recharger les partitions encore valides.
- Conserver des métadonnées indiquant le schéma du résultat Arrow, la complétude, la liste des partitions et la date de rafraîchissement de chaque partition, y compris pour un résultat vide.

Le cache doit éviter de considérer un résultat partiellement filtré comme une partition complète de la table. Pour le prototype, matérialiser complètement le résultat du plan identifié avant de le découper ; pouvoir réutiliser des partitions entre des plans différents constitue une optimisation ultérieure. Préserver les types Arrow (décimaux, dates, timestamps et fuseaux), les valeurs nulles et l'ordre en sortie lorsque le plan le garantit.

## 5. Responsabilités

| Composant | Responsabilité |
|---|---|
| `snowflake-ast` | AST, génération SQL, identification des dépendances |
| API Python | `Table`, DataFrame, `pl.Expr`, `to_arrow`, `to_polars`, `Session.builder` |
| Traducteur Polars | Traduction partagée Rust/Python vers `snowflake-ast` |
| Session Rust | Configuration, orchestration, TTL, exécution Arrow |
| Snowflake | Exécution distante et récupération des données |
| DataFusion | Lecture et exécution locale sur Parquet |
| Cache Engine | Index, partitions, invalidation, overwrite |

## 6. Plan d'implémentation

- [ ] Étendre `Session.builder.config(...)` avec `cache_dir` et `ttl`, sans modifier la création de session par profil.
- [ ] Ajouter `partition_by` comme option nommée de `to_arrow` et `to_polars` et vérifier que les appels actuels restent inchangés.
- [ ] Valider les colonnes de partitionnement.
- [ ] Générer un identifiant stable depuis le plan normalisé, ses paramètres, son partitionnement et le contexte Snowflake ; organiser les datasets par table qualifiée.
- [ ] Implémenter l'écriture Parquet avec Hive partitioning.
- [ ] Ajouter un manifeste des partitions et de leur TTL.
- [ ] Lire les partitions en cache avec DataFusion.
- [ ] Implémenter le rafraîchissement et l'overwrite sélectifs.
- [ ] Tester les cache hits, misses, expirations, partitions incomplètes, résultats vides typés et l'isolation de requêtes ou de sessions différentes.
- [ ] Comparer `to_arrow` et `to_polars`, avec et sans cache, sur les mêmes requêtes construites à partir d'une `Table` et d'expressions Polars ; vérifier les schémas, valeurs, ordre contractuel et l'absence de lecture distante sur un cache hit complet.

## 7. Points à trancher

- Comment déterminer les partitions nécessaires à partir des filtres de l'AST ?
- Comment garantir qu'une partition est complète et réutilisable entre différents plans ?
- Comment gérer les requêtes dont les résultats ne peuvent pas être rafraîchis partition par partition ?
- Comment invalider le cache si les données Snowflake changent avant expiration du TTL ?
- Faut-il prévoir un cache non partitionné à terme ?
- Comment étendre le cache aux plans multi-tables et à l'exécution par lots ?

## Principe directeur

**L'utilisateur choisit le partitionnement ; la Session gère le caching ; l'AST décrit la requête ; Snowflake et DataFusion assurent l'exécution.**

La première version doit privilégier la simplicité et la correction des résultats plutôt qu'un optimiseur de cache entièrement automatique.
