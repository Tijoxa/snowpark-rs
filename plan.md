# Plan d’implémentation et de validation de snowpark-rs

**Abandonner l’objectif de reproduire l’API Snowpark.** En Python, `Table(name)` devient le point d’entrée d’une API paresseuse proche de `pl.LazyFrame`, avec `pl.Expr` comme type d’expression public. Côté Rust, concentrer le travail sur la traduction des expressions vers `snowflake-ast` et sur la compilation en SQL Snowflake paramétré. L’exécution Arrow et Polars utilise une session explicite.

Le moteur couvre déjà les projections, les filtres, l’agrégation, le tri, la limite, les jointures internes et externes ainsi que les unions. Les suites Snowpark citées dans le plan servent à retrouver des cas limites du moteur SQL. Les signatures publiques des transformations et la sémantique des expressions traduites prennent Polars pour référence. [Sources et tests Snowpark de référence](https://github.com/snowflakedb/snowpark-python/tree/781939e92343ee4280885eb879c9eaf3ab13e59e)

## Prochaines étapes

- [x] **Intégrer le chemin Polars → SQL Snowflake.** Les filtres, projections et agrégats composent des `pl.Expr` dans `Table`. Les tests hors ligne vérifient le SQL, les binds et les expressions refusées. [API cible](#api-paresseuse-et-expressions-polars)
- [x] **Migrer l’API d’expression publique Python.** Les signatures, exemples et tests utilisent maintenant les expressions Polars. Le module `snowpark_rs.functions` et les types d’expression Snowpark ont été retirés de l’API Python.
- [x] **Accepter les expressions Polars en Rust et partager leur traduction avec Python.** La feature Cargo `polars` expose les transformations Rust sur les expressions Polars. L’extension Python utilise le même traducteur. [Traduction des expressions](#traduction-des-expressions)
- [x] **Intégrer un plugin Polars réservé à Snowflake.** Compiler `pl.col("PAYLOAD").snowflake.try_parse_json()` et vérifier son erreur en exécution locale avec Polars. [Extensions Snowflake](#extensions-snowflake)
- [x] **Tester Arrow et Polars en local.** Simuler le backend de session et vérifier le décodage Arrow IPC, les résultats vides typés, la lecture des lots, les erreurs et la conversion Polars. [Tests Python locaux](tests/test_session.py)
- [x] **Valider l’intégration sur Snowflake.** Les tests d’intégration Rust passent et les tests Python confirment la parité avec Polars sur `PUBLIC.PUBLIC.EXAMPLE`. Un test Rust vérifie aussi que le même DataFrame utilise les tables propres à chacune des deux sessions explicites. Les tests de parité s’activent via `SNOWPARK_RS_TEST_CONNECTION`. [Tests Rust](tests/snowflake_smoke.rs) · [Tests Python](tests/test_polars_snowflake.py)
- [x] **Ajouter les types, les schémas et les expressions de base.** Exposer `schema` et `columns`, les `NULL` typés, les décimaux exacts, `cast` et l’arithmétique. [Tests Rust](crates/snowflake-ast/tests/types_and_sets.rs) · [Tests Python](tests/test_dataframe.py)
- [x] **Étendre les transformations relationnelles.** Ajouter `with_column`, `rename`, `distinct`, les jointures externes et les unions par position ou par nom. [Tests Rust](crates/snowflake-ast/tests/types_and_sets.rs) · [Tests Python](tests/test_dataframe.py)
- [x] **Compiler le spread journalier via les expressions Polars.** Exécuter `spread_expr` en agrégation groupée et dans un `select()` global, sur une source au schéma de `DB_CH_FC.FC.PRICE`. [Traduction](#composition-des-expressions-polars) · [Cas de validation](#spread-journalier-avec-spread_expr)
- [x] **Traduire les clés temporelles et les prédicats sur les chaînes.** Couvrir `dt.truncate()` dans les clés de groupe et `str.contains()` en projection et en filtre. [Troncature temporelle](#clés-de-groupe-temporelles-avec-dttruncate) · [Recherche dans les chaînes](#recherche-avec-strcontains)
- [x] **Élargir la traduction Polars vers Snowflake.** Étendre `dt.truncate()` aux durées fixes et dynamiques, ajouter des opérations sur les chaînes et les séries filtrées, et exposer les conversions et fonctions semi-structurées prioritaires. [Composition](#composition-des-expressions-polars) · [Vocabulaire SQL Snowflake](#9-vocabulaire-sql-snowflake)
- [ ] **Prototyper le cache Parquet partitionné.** Configurer `cache_dir` et `ttl` via `Session.builder`, puis activer le cache avec `to_arrow(session, partition_by=[...])` ou `to_polars(session, partition_by=[...])` sur un plan construit avec `Table` et des expressions Polars. Valider les colonnes de partitionnement, stocker les partitions sous `cache_dir / "db.schema.table"` et prévoir la lecture locale avec DataFusion. [Plan du cache](plan_caching.md)

## Architecture du MVP

| Composant | Responsabilité |
| --- | --- |
| [`crates/snowflake-ast`](crates/snowflake-ast/Cargo.toml) | Cible de traduction des expressions Polars. Plans immuables, résolution des identifiants et compilation en `CompiledQuery` avec paramètres typés et ordonnés. Le crate se teste sans Python ni connexion Snowflake. |
| [`snowpark-rs` à la racine](Cargo.toml) | `DataFrame` Rust indépendant d’une session, avec délégation à `snowflake-ast`. La feature Cargo `session` ajoute les actions Arrow et leur adaptateur vers `snowflake-connector-rs`. |
| [`snowpark_rs` pour Python](pyproject.toml) | API paresseuse et adaptateur d’expressions Polars vers le plan Rust. Extension PyO3 construite par Maturin avec la feature Cargo `python`. L’extra Python `session` ajoute la session et le moteur d’exécution Arrow. |

Les transformations Rust et Python construisent le même plan immuable. `to_sql()` est disponible dans l’installation de base, sans argument de session. En Rust, il renvoie un `CompiledQuery`. En Python, il renvoie le couple `(sql, binds)`. Le même DataFrame peut ensuite être exécuté avec différentes sessions. Chaque action utilise exclusivement la session passée à l’appel. Snowflake résout les noms selon le contexte de cette session, optimise et exécute la requête.

`uv sync --dev` installe l’extension et Polars 2.1.0. L’extra `session` ajoute le backend Snowflake et Arrow.

## API paresseuse et expressions Polars

Les transformations `filter`, `select`, `with_columns`, `group_by(...).agg(...)` et `sort` utilisent les conventions de `LazyFrame` pour les formes prises en charge. Les noms des colonnes et les alias respectent leur casse. Les identifiants non cités de Snowflake se référencent en majuscules. [API Polars LazyFrame](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html) · [API et limites](README.md#python)

```python
import polars as pl
from snowpark_rs import Table

query = (
    Table("DB.PUBLIC.SALES")
    .filter(pl.col("AMOUNT") > 100)
    .select((pl.col("AMOUNT") * 2).alias("DOUBLE_AMOUNT"))
)
sql, binds = query.to_sql()
```

- [x] **Lecture déclarative.** Exposer `Table(name)` comme constructeur Python public de la source et retirer `DataFrame.table(name)`. Le constructeur s’utilise avant d’ouvrir une session et permet de chaîner les transformations DataFrame. Accepter `"DB.SCHEMA.TABLE"` et les parties séparées, comme `["DB", "SCHEMA", "TABLE"]`. Tester l’équivalence des deux formes, les noms à une, deux et trois parties ainsi que les identifiants cités qui contiennent un point, par exemple `DB.SCHEMA."TABLE.AVEC.POINT"`. Utiliser les règles d’identifiants de `Session.table` comme référence amont. [API Snowpark Python `Session.table`](https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/snowpark/api/snowflake.snowpark.Session.table)

### Traduction des expressions

- [x] **Lire l’arbre Polars.** Le traducteur parcourt les nœuds typés de `polars_plan::dsl::Expr` et produit des expressions pour le compilateur Snowflake. [Traducteur](src/polars/translate.rs)
- [x] **Exposer un adaptateur Rust typé.** La feature Cargo `polars` expose `filter_polars`, `select_polars`, `with_columns_polars`, `group_by_polars`, `agg_polars` et `sort_polars`. Les expressions sont conservées dans le plan jusqu’à la compilation SQL, la résolution du schéma ou l’exécution. Les builds Rust par défaut et `snowflake-ast` restent indépendants de Polars. [API Rust](src/polars.rs) · [Tests Rust](tests/dataframe.rs)
- [x] **Réutiliser le même traducteur dans l’extension Python.** Le pont PyO3 importe `pl.Expr` avec `pyo3-polars::PyExpr` au moment de la compilation. La feature `python` utilise la révision et le jeu de fonctionnalités du runtime 32 bits de Polars 2.1.0 pour le format binaire et l’ABI du plugin. Les tests Python et Rust comparent le SQL, l’ordre des paramètres et les diagnostics de traduction. [Pont PyO3](src/python.rs) · [Tests Python](tests/test_dataframe.py) · [Source de `PyExpr`](https://github.com/pola-rs/polars/blob/py-2.1.0/pyo3-polars/pyo3-polars/src/types.rs)
- [x] **Vérifier les traductions hors ligne.** Les tests couvrent les colonnes, alias, littéraux paramétrés, comparaisons, expressions arithmétiques, conversions prises en charge, sélecteurs et l’ordre explicite des `NULL`. La somme produit `COALESCE(SUM(...), 0)` pour les entrées vides ou entièrement nulles. La comparaison des valeurs et types Snowflake reste dans le jalon d’intégration. [Tests Python](tests/test_dataframe.py)
- [x] **Signaler les traductions non prises en charge.** Lever `UnsupportedPolarsExpression`, une sous-classe de `RuntimeError`, lors de `to_sql()` ou de la compilation déclenchée par une action, avant l’envoi à Snowflake. Indiquer la transformation, le nœud ou la combinaison en cause et la raison, par exemple un sélecteur qui exige le schéma ou une fenêtre dont l’ordre est indéterminé. Tester aussi les expressions inconnues imbriquées et la distinction avec les erreurs d’exécution Snowflake.

### Composition des expressions Polars

Le traducteur distingue `Scalar`, `Row` et `Aggregate`. Dans `col("VALUE").filter(predicat).drop_nulls().sort().tail(k).mean()`, il conserve le prédicat qui définit la série candidate, puis utilise une projection intermédiaire pour son rang et la taille calculée par groupe. La moyenne s’applique aux valeurs candidates dont le rang entre dans la tranche. [Traducteur partagé](src/polars/translate.rs) · [Réductions](src/polars/translate/reductions.rs)

- [ ] **Analyser chaque sous-expression avant de produire du SQL.** Suivre son type, sa cardinalité et sa portée. Distinguer une constante pour la requête, une valeur par ligne, une valeur par groupe et une série par groupe dont la longueur ou l’ordre peuvent changer. Évaluer la réutilisation des propriétés de l’IR de la version Polars épinglée, notamment `is_scalar` et `is_length_preserving`, et vérifier la compilation des tables au schéma inconnu. Pour une série, conserver sa source, l’alignement avec les autres séries et l’ordre issu d’un tri explicite. Utiliser ces informations pour vérifier les clés de groupe, les prédicats, la diffusion d’un scalaire et les combinaisons de branches. [Analyse actuelle](src/polars/translate.rs) · [Propriétés de `ExprIR` dans Polars](https://github.com/pola-rs/polars/blob/7f902bd3ffd27ff82d14278a09a295ff2f7aed92/crates/polars-plan/src/plans/expr_ir.rs#L275-L289)
- [x] **Composer les opérations utilisées par le spread.** La traduction couvre `when/then/otherwise`, `//` sur des entiers, `floor`, `fill_null`, `min_horizontal` numérique, `mean`, et `any` et `all` avec la règle Polars par défaut qui ignore les `NULL`.
- [x] **Compiler les réductions sur les candidats.** `filter` et `drop_nulls` définissent les lignes candidates. Sous une réduction, `sort().head(k)` et `sort().tail(k)` utilisent `ROW_NUMBER` avec les candidats en tête du classement. Les fenêtres calculant `k` utilisent le groupe complet. Les conditions de sélection conservent les groupes sans candidat et les clés `NULL`. [Réductions](src/polars/translate/reductions.rs) · [Snowflake `ROW_NUMBER`](https://docs.snowflake.com/en/sql-reference/functions/row_number)
- [x] **Distinguer la longueur d’une série filtrée du nombre de valeurs non nulles.** `Expr.len()` compte toutes les lignes candidates et `Expr.count()` compte les valeurs non nulles. Un prédicat par ligne doit conserver l’alignement avec la série qu’il filtre.
- [ ] **Valider les compositions et les limites prises en charge.** Partager le chemin entre `select()` avec une réduction globale et `group_by(...).agg(...)`. Tester `mean` filtrée, `count` après `drop_nulls`, `when` sur une réduction, puis les sélections triées avec un `k` fixe et avec un `k` calculé par groupe. Couvrir les ex æquo lorsque la ligne retenue influe sur le résultat, les séries de longueurs différentes et plusieurs réductions portant sur des ensembles distincts. Vérifier d’abord le SQL, les types et l’ordre des binds hors ligne, puis les résultats face à Polars. Les compositions sans équivalence SQL établie doivent indiquer le nœud et la contrainte en cause dans `UnsupportedPolarsExpression`.

### Comparaisons temporelles et appartenance

- [x] **Comparer les dates et les datetimes.** Traduire les dates Python,
  les littéraux `datetime`, `pl.date` et les constructeurs temporels constants.
  Les comparaisons entre date et datetime utilisent minuit, en UTC pour les
  timestamps zonés. Le schéma local permet de valider les comparaisons entre
  colonnes. [Tests](tests/test_polars_temporal_membership.py)
- [x] **Exporter les timestamps zonés.** Convertir `TIMESTAMP_TZ` en
  `timestamp[ns, tz=UTC]` dans Arrow et en `datetime[ns, UTC]` dans Polars.
  Aligner les sélecteurs Polars sur les types Arrow de précision par défaut
  pour `TIMESTAMP_TZ`, `TIMESTAMP_LTZ` et `TIMESTAMP_NTZ`.
- [x] **Traduire `is_in`.** Accepter les listes et les séries Polars
  implodées. Dans un filtre positif, accepter une requête à une colonne
  passée directement ou via `implode()`. Vérifier l’arité au moment de la
  compilation si le schéma est connu, et laisser Snowflake la vérifier lors
  de l’exécution d’une lecture dont le schéma est inconnu.

```python
from datetime import date

source = Table("PUBLIC.PUBLIC.EXAMPLE")
source.filter(pl.col("CREATED_AT") >= pl.date(2024, 1, 1)).to_polars(session)
source.filter(pl.col("CREATED_AT") >= date(2024, 1, 1)).to_polars(session)

ids = source.filter(pl.col("CATEGORY") == "A").select("ID")
source.filter(pl.col("ID").is_in(ids.implode())).to_polars(session)
source.filter(pl.col("ID").is_in(ids)).to_polars(session)
```

### Clés de groupe temporelles avec `dt.truncate()`

- [x] **Traduire les périodes constantes.** `dt.truncate()` accepte les unités de la nanoseconde à l’année, leurs multiples, les combinaisons compatibles avec Polars et `timedelta`. Les semaines s’alignent sur le lundi 5 janvier 1970. Une colonne déclarée `DATE` conserve son type. [Snowflake `DATE_TRUNC`](https://docs.snowflake.com/en/sql-reference/functions/date_trunc) · [Snowflake `TIME_SLICE`](https://docs.snowflake.com/en/sql-reference/functions/time_slice) · [Snowflake `DATEADD`](https://docs.snowflake.com/en/sql-reference/functions/dateadd)
- [x] **Tester les clés de groupe et les composants temporels.** Couvrir la compilation des clés, la conservation du type, `dt.date()` et `dt.minute()`. Le test de parité Snowflake couvre le type et le nom de la clé, les timestamps nuls, les effectifs et les transitions d’heure dans `Europe/Paris`. [Tests hors ligne](tests/test_polars_temporal_membership.py) · [Test Snowflake](tests/test_polars_snowflake.py)
- [x] **Traduire les durées dynamiques.** Le SQL interprète les durées textuelles par ligne, y compris dans une clé de groupe. Une durée `NULL` produit `NULL`. Les durées invalides signalent une erreur à la compilation pour les littéraux, et à l’exécution pour les valeurs dynamiques.

### Recherche avec `str.contains()`

- [x] **Traduire la recherche littérale.** `literal=True` accepte un motif constant lié comme paramètre ou un motif calculé par ligne. `POSITION` utilise la collation UTF-8 explicite et garantit la correspondance du motif vide sur toute chaîne non nulle. Les tests couvrent les métacaractères, la casse, les accents, les collations et les `NULL`. [Snowflake `POSITION`](https://docs.snowflake.com/en/sql-reference/functions/position) · [Tests](tests/test_polars_string_contains.py)
- [x] **Traduire la recherche regex de sous-chaîne.** `REGEXP_INSTR(...) > 0` recherche un motif constant du sous-ensemble commun, avec prise en charge du préfixe `(?i)`. La traduction traite aussi les correspondances de longueur zéro sur un sujet vide. Un motif invalide lève une erreur en mode strict et donne `NULL` en mode non strict. Les motifs regex dynamiques et les constructions hors du sous-ensemble produisent un diagnostic explicite. Les tests couvrent les projections et les filtres. [Snowflake `REGEXP_INSTR`](https://docs.snowflake.com/en/sql-reference/functions/regexp_instr) · [Sémantique Polars de `literal` et `strict`](https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.str.contains.html) · [Test Snowflake](tests/test_polars_snowflake.py)

### Spread journalier avec `spread_expr`

Cette expression compile en SQL Snowflake dans `group_by(...).agg(...)` et dans `select()`. Elle combine des agrégats scalaires par groupe et des moyennes sur des séries filtrées, triées et découpées avec une longueur calculée dans le même groupe.

```python
import polars as pl


def spread_expr(nb_hours: int, period: int | pl.Expr | None = None) -> pl.Expr:
    if period is None:
        period_expr = pl.when((pl.col("TS").dt.minute() != 0).any()).then(15).otherwise(60)
    elif isinstance(period, int):
        if period <= 0:
            raise ValueError("Period must be positive")
        period_expr = pl.lit(period)
    else:
        period_expr = period

    n_elem = 60 // period_expr
    n_elem = pl.when(n_elem.is_in((1, 4))).then(n_elem).otherwise(None)
    k = (nb_hours * n_elem).floor().cast(pl.Int64)

    sym = pl.col("VALUE").drop_nulls().sort()
    up = pl.col("VALUE").filter(pl.col("DIRECTION") == "UP").drop_nulls().sort()
    down = pl.col("VALUE").filter(pl.col("DIRECTION") == "DOWN").drop_nulls().sort()

    sym_spread = pl.when((k > 0) & (sym.count() >= k)).then(sym.tail(k).mean() - sym.head(k).mean()).otherwise(None)
    k_asym = pl.min_horizontal(k.fill_null(0), up.count(), down.count())
    asym_spread = pl.when(k_asym > 0).then(up.tail(k_asym).mean() - down.head(k_asym).mean()).otherwise(None)
    return pl.when(pl.col("DIRECTION").eq("SYMMETRIC").all()).then(sym_spread).otherwise(asym_spread).alias("VALUE")
```

- [x] **Compiler l’expression complète.** Les tests hors ligne compilent `spread_expr(4)` dans une agrégation groupée et dans un `select()` global, avec `period=None`, un entier et une expression scalaire par groupe. [Tests](tests/test_polars_snowflake.py)
- [x] **Tester les périodes de 20 et 45 minutes.** Le calcul valide le quotient `60 // period_expr`, qui vaut `1` pour 45 minutes. Pour 20 minutes, la condition finale produit `NULL` sur Snowflake. Polars 2.1.0 lève `ComputeError` en évaluant le décalage nul de `tail(k)`.

#### Table et données de test

Le DDL de référence décrit `DB_CH_FC.FC.PRICE`. Le test d’intégration utilise une CTE `SNOWPARK_RS_PRICE_FIXTURE` avec les mêmes colonnes et des données synthétiques.

```sql
CREATE TABLE DB_CH_FC.FC.PRICE (
    TS TIMESTAMP_LTZ(9) COMMENT 'Prediction timestamp',
    MARKET VARCHAR(16777216) COMMENT 'Market name, ex: "DA", "MFRR_ENERGY"',
    VALUE FLOAT COMMENT 'Prediction value',
    UNIT VARCHAR(16777216) COMMENT 'Unit, ex: "EUR/MWh", "EUR/MW/h"',
    ZONE VARCHAR(16777216) COMMENT 'Zone, ex: "FR", "PL", "IT_CALA", "DK_1"',
    FORECAST_PERIOD VARCHAR(16777216) COMMENT 'Forecast report period, ex: "Y2025S2", "Y2026S1"',
    SCENARIO VARCHAR(16777216) COMMENT 'Scenario, ex: "Central", "Low", "High"',
    ID_MODEL VARCHAR(16777216) COMMENT 'Id for the model, ex: "XGBRegressor"',
    VERSION NUMBER(38,0) COMMENT 'Version, ex: 0, 1, 2',
    ID_INFERENCE_DATASET VARCHAR(16777216) COMMENT 'Id for the inference dataset, ex: "future_covariates_PL_V2"',
    DIRECTION VARCHAR(16777216) COMMENT 'Direction, ex: "UP", "DOWN", "SYMMETRIC"',
    UPLOAD_TIMESTAMP TIMESTAMP_LTZ(9) COMMENT 'Entry upload timestamp',
    ID_FLEX VARCHAR(16777216) COMMENT 'Id for the flexibility, ex: "flex_00", "flex10", "postpone_flex_2y_10"'
)
CLUSTER BY (FORECAST_PERIOD, ZONE, MARKET, ID_MODEL, SCENARIO, VERSION);
```

- [x] **Comparer les résultats à Polars.** Le test utilise les mêmes lignes et `spread_expr` dans les deux moteurs. Il groupe par jour, marché et zone, compare les clés, le type `FLOAT`, les `NULL` et les valeurs. Il couvre aussi le groupe global sur des entrées présentes ou vides. Le profil se configure via `SNOWPARK_RS_TEST_CONNECTION`. [Tests de parité Snowflake](tests/test_polars_snowflake.py)
- [x] **Couvrir les cas métier.** Le test couvre `k = 4` en horaire et `k = 16` en quart d’heure pour quatre heures, les périodes fixes, dynamiques et inférées, `k = 0`, les timestamps manquants, les prix nuls et les directions nulles. Il inclut les six prix symétriques de `1` à `6`, le spread asymétrique calculé sur deux prix de chaque côté et les groupes sans assez de candidats.
- [x] **Vérifier les changements d’heure.** Le test compare les jours locaux de 23 et de 25 heures dans `Europe/Paris`, avec une période inférée. [Sémantique de `TIMESTAMP_LTZ`](https://docs.snowflake.com/en/sql-reference/data-types-datetime)

### Extensions Snowflake

- [x] **Ajouter le namespace `.snowflake` aux expressions Polars.** Reprendre le modèle de `.cactus` dans le projet `polars-cactus`, avec `register_expr_namespace` et `register_plugin_function`. La fonction du namespace renvoie un vrai `pl.Expr`. Le compilateur reconnaît le nœud `FfiPlugin` via l’identité de la bibliothèque et un symbole autorisé, puis traduit ses arguments récursivement. Commencer par `TRY_PARSE_JSON` et tester la composition sous un alias et un test de nullité. [Plugins d’expression Polars](https://docs.pola.rs/user-guide/plugins/expr_plugins/) · [Snowflake `TRY_PARSE_JSON`](https://docs.snowflake.com/en/sql-reference/functions/try_parse_json)
- [x] **Permettre l’import `import snowpark_rs as pls`.** Réexporter `pl.col` sous `pls.col`, sur le modèle de `plc.col`, pour écrire `pls.col("PAYLOAD").snowflake.try_parse_json()`. Exposer aussi `pls.try_parse_json("PAYLOAD")` comme raccourci, avec un nom de colonne ou un `pl.Expr` en entrée. Faire construire le même nœud de plugin par les deux formes et vérifier qu’elles produisent le même SQL et les mêmes paramètres.
- [x] **Échouer explicitement en exécution locale.** Fournir des symboles de plugin valides dont l’évaluation par Polars renvoie une erreur indiquant qu’une `Table` Snowflake est requise. Un symbole absent a provoqué un panic lors de l’essai. Tester le rejet des plugins tiers par le compilateur Snowflake et l’erreur locale du plugin Snowflake.

## Références du compilateur Snowflake

- `Column` encapsule une expression. `DataFrame.filter()` produit un plan `Filter` ou transforme son `SelectStatement`. Le DataFrame Python appelle déjà l’analyseur lors de sa construction. En Rust, on reprend la structure en expressions et plans, avec une compilation déclenchée par `to_sql()` ou une action. [Column](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/src/snowflake/snowpark/column.py#L248-L315) · [DataFrame](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/src/snowflake/snowpark/dataframe.py#L665-L686) · [filter](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/src/snowflake/snowpark/dataframe.py#L2372-L2415)
- `Analyzer.resolve()` descend dans les enfants du plan avant de résoudre le parent. `SelectStatement` rend les clauses et peut fusionner un filtre avec le bloc existant si les dépendances de colonnes et les clauses le permettent. En Rust, conserver la frontière de sous-requête dès que la fusion n’est pas démontrée correcte. [Analyzer.resolve](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/src/snowflake/snowpark/_internal/analyzer/analyzer.py#L966-L1008) · [Rendu SQL](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/src/snowflake/snowpark/_internal/analyzer/select_statement.py#L1162-L1211) · [Fusion de filtres](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/src/snowflake/snowpark/_internal/analyzer/select_statement.py#L1721-L1765)
- Le `Query` Python porte du SQL et des paramètres. `collect()` peut aussi sérialiser un AST Protobuf, que la connexion ajoute à la dernière requête en tant que paramètre distinct. Le MVP Rust reprend le contrat SQL et paramètres du plan d’exécution. Il utilise des enums Rust internes pour représenter les opérations. [Query](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/src/snowflake/snowpark/_internal/analyzer/snowflake_plan.py#L2539-L2569) · [collect](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/src/snowflake/snowpark/dataframe.py#L877-L904) · [Exécution](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/src/snowflake/snowpark/_internal/server_connection.py#L904-L937)

## Représentation Rust

Commencer avec un plan interne de petite taille. Le MVP construit des requêtes depuis l’API DataFrame et connaît les quelques opérations qu’il sait compiler. Reporter le choix de `datafusion-expr` à un besoin concret de réutilisation de son modèle de types ou d’expressions, et celui de `sqlparser` à un besoin d’analyse de SQL en entrée.

| Type | Première responsabilité |
| --- | --- |
| `Ident`, `ObjectName` | Représenter un identifiant et un nom de table en une à trois parties. Les parties d’un nom qualifié sont fournies séparément. |
| `Column`, `Expr` | Types du compilateur utilisés par l’adaptateur Polars. `Expr` porte les références, les littéraux, les conversions, l’arithmétique, les comparaisons, la logique et les tests de nullité. |
| `Field`, `DataType`, `Decimal` | Représenter un champ, les types Snowflake disponibles et les valeurs fixes exactes. |
| `Projection`, `SortKey` | `Projection` attache un alias à une expression. `SortKey` portera une direction et un placement des `NULL`. |
| `LogicalPlan` | Lectures, filtres, projections, remplacements et renommages, déduplication, agrégations, tris, limites, jointures et unions. Chaque transformation référence son ou ses enfants via `Arc<LogicalPlan>`. |
| `CompiledQuery` | Contenir la chaîne SQL et un `Vec<BindValue>` dans l’ordre des placeholders. Le compilateur et ce type restent indépendants du connecteur. |

Dans `snowflake-ast`, une branche dérivée partage le plan parent et construit son propre nœud. Le DataFrame Rust de `snowpark-rs` porte uniquement l’état du plan. Modéliser les opérateurs et les agrégats supportés avec des enums. Réserver dès le départ les alias aux projections et aux sorties d’agrégat. Une expression calculée doit avoir un alias explicite si le plan parent doit pouvoir la référencer.

## Compilation Snowflake

1. **Compiler de bas en haut.** Chaque nœud reçoit un bloc SQL enfant et les informations disponibles sur ses sorties. Le premier compilateur couvre une lecture de table, un filtre et une projection. Un bloc simple peut rendre `SELECT … FROM … WHERE …`. Pour filtrer un alias calculé ou filtrer après une limite, créer un bloc externe et référencer les colonnes exportées par le bloc interne.
2. **Résoudre les colonnes dans la bonne portée.** Pour une lecture à une seule source dont le schéma est inconnu, qualifier les références sur cette source et laisser Snowflake valider leur existence. Après une projection explicite, les noms exportés sont connus et le compilateur peut rejeter une référence supprimée. Au jalon jointure, utiliser des références liées au côté d’entrée et construire une sortie aux noms uniques. Une référence non qualifiée ambiguë produit une erreur de compilation. Les alias SQL générés sont déterministes et uniques dans leur bloc.
3. **Séparer noms et valeurs.** Valider la forme des identifiants non cités et les convertir en majuscules avant de les entourer de guillemets. Ainsi `id` est rendu en `"ID"`, et l’identifiant explicitement cité `"id"` conserve sa casse. Doubler tout guillemet interne. Un point dans un identifiant cité reste dans la même partie du nom. Le MVP vise le comportement par défaut de `QUOTED_IDENTIFIERS_IGNORE_CASE`. [Règles des identifiants Snowflake](https://docs.snowflake.com/en/sql-reference/identifiers-syntax)
4. **Émettre les paramètres avec le SQL.** Produire un `?` par valeur et assembler les fragments dans l’ordre textuel de la requête finale. Si une projection externe contient un `?` avant une sous-requête `FROM` qui en contient un autre, la valeur de la projection passe en premier. Rendre les parenthèses explicites et préserver la logique SQL à trois valeurs avec `IS NULL` et `IS NOT NULL`. Pour les `NULL` typés et les décimaux exacts, compiler une conversion vers le type Snowflake visé et transmettre la valeur paramétrée au connecteur.
5. **Préserver l’ordre des opérations.** Un filtre sur la sortie d’un agrégat passe d’abord dans un bloc externe. Un tri exposé au consommateur doit apparaître dans le bloc le plus externe. Au MVP, accepter `sort` en fin de chaîne, éventuellement suivi de `limit` dans le même bloc, et signaler les compositions avec tri intermédiaire dont la sémantique reste à définir. Rendre `NULLS FIRST` ou `NULLS LAST` explicitement pour les clés de tri. [ORDER BY Snowflake](https://docs.snowflake.com/en/sql-reference/constructs/order-by)

## Validation avec PUBLIC.PUBLIC.EXAMPLE

Les tests d’intégration Rust et Python lisent cette table avec ses cinq lignes. Pour la préparer dans un environnement de test vide, créer la table puis insérer les lignes suivantes. Conserver `CREATED_AT` de type `DATE` et ajouter les trois variantes de timestamp à la précision nanoseconde utilisées par les tests Python. Les timestamps de cette fixture couvrent volontairement des instants de part et d’autre du 1er janvier 2024, des fractions de seconde et plusieurs décalages UTC, indépendamment des dates historiques de `CREATED_AT`. Les autres jeux de données du plan utilisent des objets temporaires propres aux tests.

```sql
CREATE TABLE PUBLIC.PUBLIC.EXAMPLE (
    id              INT,
    name            STRING,
    category        STRING,
    amount          NUMBER(10,2),
    created_at      DATE,
    created_at_tz   TIMESTAMP_TZ(9),
    created_at_ltz  TIMESTAMP_LTZ(9),
    created_at_ntz  TIMESTAMP_NTZ(9)
);

INSERT INTO PUBLIC.PUBLIC.EXAMPLE (
    id, name, category, amount, created_at,
    created_at_tz, created_at_ltz, created_at_ntz
)
SELECT
    id, name, category, amount, TO_DATE(created_at, 'YYYY-MM-DD'),
    TO_TIMESTAMP_TZ(ts || ' ' || tz, 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'),
    TO_TIMESTAMP_TZ(ts || ' ' || tz, 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM')::TIMESTAMP_LTZ(9),
    TO_TIMESTAMP_NTZ(ts, 'YYYY-MM-DD HH24:MI:SS.FF9')
FROM VALUES
    (1, 'Alice',   'A', 120.50, '2026-01-15', '2023-12-31 23:59:59.999999999', '+00:00'),
    (2, 'Bob',     'B',  89.99, '2026-02-03', '2024-01-01 00:00:00.123456000', '+00:00'),
    (3, 'Charlie', 'A', 240.00, '2026-03-21', '2024-01-01 01:00:00.123456001', '+01:00'),
    (4, 'Diana',   'C',  15.75, '2026-04-10', '2026-04-10 09:15:00.000000000', '-04:00'),
    (5, 'Ethan',   'B', 310.20, '2026-05-28', '2026-05-28 18:45:00.000000000', '+02:00')
AS fixture(id, name, category, amount, created_at, ts, tz);
```

Le premier cas construit le plan `Project([id, name], Filter(category = lit("A"), Scan(PUBLIC.PUBLIC.EXAMPLE)))`. Le test hors ligne vérifie le SQL et le paramètre suivants.

```sql
SELECT t0."ID", t0."NAME"
FROM "PUBLIC"."PUBLIC"."EXAMPLE" AS t0
WHERE t0."CATEGORY" = ?
```

Le seul paramètre est `BindValue::Text("A")`. Le test d’intégration vérifie les colonnes `ID` et `NAME` ainsi que les deux lignes `(1, "Alice")` et `(3, "Charlie")`, indépendamment de leur ordre.

Pour le jalon agrégation, réutiliser la table avec `group_by(category).agg(sum(amount).alias("total")).sort(category.asc())`. Vérifier les totaux décimaux exacts `A = 360.50`, `B = 400.19` et `C = 15.75`, dans cet ordre.

Le test [de parité temporelle Python](tests/test_polars_snowflake.py) compare les filtres `>= pl.date(2024, 1, 1)` sur les quatre colonnes temporelles et `<= datetime(..., tzinfo=UTC)` sur `CREATED_AT_TZ` avec les mêmes opérations exécutées localement dans Polars. Il vérifie également les types Polars `Datetime("ns", "UTC")` pour les timestamps `TZ` et `LTZ`, et `Datetime("ns")` pour `NTZ`, ainsi que le type Arrow `timestamp[ns, tz=UTC]` de `CREATED_AT_TZ`. La fixture sert aussi à vérifier `is_in` avec une sous-requête mono-colonne et le rejet d’une sous-requête multi-colonnes.

## Session explicite et exécution Arrow

### Interface publique visée

```python
import polars as pl
from snowpark_rs import Table

df = Table("DB.SCHEMA.TABLE").filter(pl.col("CATEGORY") == "A")
sql, binds = df.to_sql()

from snowpark_rs import Session

session = Session.builder.config("connection_name", "myconnection").create()
table = df.to_arrow(session)
batches = df.to_arrow_batches(session)
```

| Méthode | Python | Rust |
| --- | --- | --- |
| `to_sql()` | `to_sql() -> tuple[str, list[BindValue]]` | `Result<CompiledQuery, CompileError>` |
| `to_arrow(session)` | Un `pyarrow.Table` matérialisé | Action asynchrone avec `&Session` qui renvoie un `RecordBatch` matérialisé |
| `to_arrow_batches(session)` | Un itérateur de `pyarrow.Table` | Action asynchrone avec `&Session` qui renvoie un flux de `RecordBatch`, avec erreurs de lecture par élément |

La session est un argument obligatoire des deux actions Arrow. Chacune soumet sa propre requête avec ses paramètres. Le résultat par lots se lit au fur et à mesure et conserve les ressources du curseur jusqu’à la fin de l’itération ou à sa libération. `to_arrow()` matérialise le même flux et produit une table vide avec le schéma attendu si la requête ne renvoie aucune ligne.

### Installation et création de session

- [x] **Feature Cargo `session`.** Rendre `snowflake-connector-rs` et les dépendances Arrow nécessaires optionnelles. Activer `Session` et les deux actions Arrow avec `--features session`. Retirer la durée de vie de session et le champ de session du DataFrame. Retirer `with_session()` et `collect()` de l’API publique du crate racine et migrer les tests correspondants.
- [x] **Extra Python `session`.** Le paquet de base fournit la compilation hors ligne. L’installation de `snowpark-rs[session]` ajoute PyArrow, les données de fuseaux horaires et le paquet compagnon `snowpark-rs-session` de même version. Celui-ci contient le backend natif compilé avec la feature Cargo `session`. Le paquet de base lui transmet le SQL et les binds typés par une interface interne Python. Les tests d’installation vérifient l’import de `Session`, les actions Arrow et l’absence du backend dans une installation de base. Un appel Arrow dans l’installation de base indique comment activer l’extra. [Spécification des extras Python](https://packaging.python.org/en/latest/specifications/core-metadata/#provides-extra-multiple-use)
- [x] **Profil nommé.** Exposer `Session.builder` et la chaîne `.config("connection_name", "myconnection").create()`. Choisir le répertoire de configuration selon la convention Snowflake. Utiliser `SNOWFLAKE_HOME` s’il est défini ou `~/.snowflake` par défaut. Si le répertoire choisi n’existe pas, utiliser celui de la plateforme. Charger le profil nommé depuis `connections.toml`. Encapsuler la `Session` de `snowflake-connector-rs` et lui déléguer l’authentification, la configuration Snowflake et la connexion. Avec la version actuelle du connecteur, adapter le profil vers `ClientConfig` et `SessionConfig`. Tester le profil absent ou invalide, les authentifications prises en charge et la remontée d’erreur sans valeurs secrètes. [Création d’une session Snowpark à partir d’un profil](https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-session#connect-by-using-the-connections-toml-file)

### Exécution et conversion

- [x] **Soumission.** L’[adaptateur du crate racine](src/session/mod.rs) construit le statement et envoie les paramètres typés dans l’ordre de `CompiledQuery`. Les erreurs distinguent compilation, soumission, lecture et conversion Arrow. Elles conservent le code Snowflake et l’identifiant de requête lorsqu’ils sont disponibles. [Statement et binds](https://github.com/estie-inc/snowflake-connector-rs/blob/cf11ce539849c9124ede4b952a643c2700588573/src/statement/builder.rs#L157-L205)
- [x] **Lecture incrémentale.** La version `1.1.1` de `snowflake-connector-rs` rejette le format Arrow côté serveur. Avec cette version, convertir le schéma et les tables partielles renvoyées par `ResultCursor::next_table()` en lots Arrow côté Rust. Préserver l’ordre des colonnes et des lots, les valeurs nulles, la précision des décimaux et les métadonnées temporelles. Tester les résultats vides, les lots multiples, l’arrêt anticipé et les erreurs de lecture en cours d’itération. Une future prise en charge native du format Arrow dans le connecteur pourra remplacer cette conversion. [Formats acceptés](https://github.com/estie-inc/snowflake-connector-rs/blob/cf11ce539849c9124ede4b952a643c2700588573/src/statement/executor.rs#L145-L162) · [Curseur et lots](https://github.com/estie-inc/snowflake-connector-rs/blob/cf11ce539849c9124ede4b952a643c2700588573/src/result_cursor/cursor.rs#L97-L178)

## 1. Lecture, filtre, projection et exécution

- [x] `table → filter → select → to_sql` avec plans immuables, identifiants cités ou normalisés, alias de projection, comparaisons, booléens, tests de nullité et paramètres chaîne, entier et booléen. Les tests couvrent les noms qualifiés, les caractères spéciaux, le parent partagé, les références absentes ou ambiguës et l’ordre des paramètres entre clauses. [Tests Rust hors ligne](crates/snowflake-ast/tests/dataframe.rs)
- [x] Tests d’exécution migrés vers les deux actions Arrow avec une session passée à l’appel. Ils couvrent le schéma `ID, NAME`, les lignes Alice et Charlie, le résultat vide avec schéma pour `O'Neil` et une composition avec alias et paramètres de trois types. [Tests Rust d’intégration](tests/snowflake_smoke.rs)
- [x] Exposer la lecture, le filtre, la projection et `to_sql()` en Python. Vérifier avec `pytest` les projections mixtes, l’ordre et le type des paramètres Python, la conversion des erreurs de compilation et les limites des entiers signés sur 64 bits. [Bindings](src/python.rs) · [Tests Python](tests/test_dataframe.py)
- [x] **Compléter les régressions de base.** Vérifier les tables en une, deux et trois parties, les noms Unicode et les collisions de casse avec les règles Snowflake par défaut, les identifiants de 255 caractères et le rejet à 256 caractères, la logique à trois valeurs sur de vraies cellules `NULL` et les cas où `?` appartient à un identifiant cité. Confirmer qu’aucune erreur locale de compilation ne déclenche d’envoi SQL et que les erreurs serveur, de lecture/décodage et de conversion Arrow conservent leur étape, leur identifiant de requête et leur cause. [Tests AST](crates/snowflake-ast/tests/dataframe.rs) · [Connecteur simulé](tests/session.rs) · [Tests Snowflake](tests/snowflake_smoke.rs) · [Tests Python](tests/test_dataframe.py) · [Session Python](tests/test_session.py)
  - Références Snowpark Python : [Identifiants](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_column_names.py) · [Quoting](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/unit/test_analyzer_utils_quoting.py) · [Valeurs NULL](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/mock/test_null_values.py) · [Connexion](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/unit/test_server_connection.py)

## 2. Jalon relationnel

Les nœuds et les règles de compilation sont implémentés dans `snowflake-ast` et exposés par le DataFrame Rust et les bindings PyO3. Les tests hors ligne vérifient le SQL et l’ordre des paramètres. Les tests d’intégration couvrent les résultats Arrow sur Snowflake.

- [x] **Agrégation.** Avant les agrégats avancés, implémenter `group_by(...).agg(...)`, `agg(...)` global, `COUNT(*)`, `COUNT(col)` et `SUM(col)`. Exporter les clés et les alias, puis compiler filtre et projection sur ces sorties. Tester la distinction entre `COUNT(*)` et `COUNT(col)` sur des `NULL`, la somme vide ou entièrement nulle, le total décimal exact par catégorie sur `EXAMPLE`, plusieurs agrégats dans le même bloc et le rejet d’une colonne source masquée. [Agrégations](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_df_aggregate.py) · [Paramètres](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_bind_variable.py)
- [x] **Tri et limite.** Ajouter `sort`, plusieurs clés `ASC` et `DESC`, un placement explicite de `NULL`, puis `limit(n)` avec offset optionnel. Tester les égalités avec une clé secondaire, les deux placements de `NULL`, `limit(0)`, une limite au-delà de la cardinalité, le tri suivi d’une limite dans le même bloc et `limit → filter` dans un bloc externe. Tester le diagnostic pour un tri intermédiaire dont le contrat reste à définir et réutiliser ces cas quand cette composition sera supportée. [Tri](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_df_sort.py) · [Limite et filtres imbriqués](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_nested_select_plan_analysis.py#L314)
- [x] **Jointure interne.** Ajouter `INNER JOIN ... ON` avec deux entrées à projection explicite, références liées au côté et noms de sortie uniques. Garantir un alias SQL par relation dans le plan partagé. Tester deux colonnes homonymes référencées par côté, un nom ambigu sans qualification, un alias réutilisé hors de sa portée, une auto-jointure correctement qualifiée, la cardinalité avec clés dupliquées et `NULL`, et les binds de la projection externe, des deux entrées et du `ON`. [Jointures](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_dataframe_join_suite.py) · [Paramètres](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_bind_variable.py)

## 3. Expressions, types et schémas

- [x] **Métadonnées locales et serveur.** `schema` et `columns` exposent les sorties nommées connues du plan. Déclarer si besoin le schéma ordonné de la table. Le type et la nullabilité d’un champ restent indéterminés quand ils dépendent de Snowflake. `schema_with_session(session)` lit le schéma Arrow du serveur avec une requête à zéro ligne.
- [x] **Littéraux et opérations de base.** Les paramètres acceptent les décimaux exacts et les `NULL` typés. Exposer `cast`, `+`, `-`, `*`, `/`, `%`, la négation et l’exponentiation. Tester la précédence, la précision, la validation des types et l’ordre des paramètres. [Arithmétique et précision](https://docs.snowflake.com/en/sql-reference/operators-arithmetic)

- [ ] **Types de données et valeurs.** Étendre la représentation des littéraux, des paramètres et des résultats aux flottants, dates, heures, timestamps NTZ/LTZ/TZ et binaires, puis aux intervalles, `VARIANT`, `ARRAY`, `OBJECT`, types structurés, vecteurs, géographie, géométrie et `FILE`. Pour chaque type, vérifier les capacités du connecteur, puis tester l’aller-retour, la nullabilité, les bornes, précision/échelle, les fuseaux, les valeurs spéciales et la différence entre `NULL` SQL et `null` JSON. [Types documentés](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/docs/source/snowpark/types.rst) · [Littéraux](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_literal_suite.py) · [Types structurés](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_infer_schema_structured_types.py)
- [ ] **Expressions Polars complémentaires.** `when/then/otherwise`, la concaténation horizontale, les préfixes, les suffixes et les longueurs de chaîne sont couverts. Poursuivre avec les opérations Polars sur les collections et les compositions supplémentaires. [Expressions Polars](https://docs.pola.rs/api/python/stable/reference/expressions/index.html)
- [ ] **Appels de fonctions SQL.** Ajouter à l’AST les appels nécessaires aux traductions Polars et au namespace `.snowflake`. Valider le nom SQL et tester l’arité, les arguments facultatifs, l’ordre des paramètres, le type de retour et le traitement des `NULL`. Prioriser les fonctions utiles aux expressions traduites et aux premiers plugins. [Fonctions Snowflake](https://docs.snowflake.com/en/sql-reference/functions-all)
- [ ] **Schéma et projections résolues.** Résoudre les étoiles et étendre l’inférence des types locaux. Tester une sélection vide de lignes avec schéma intact, l’ordre des champs, les noms cités, les colonnes en double, la qualification et les types de sortie inférés par Snowflake. [Schéma de résultat](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_result_schema_suite.py) · [Analyse imbriquée](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_nested_select_plan_analysis.py)

## Fonctions du moteur et pistes à réévaluer

Les jalons suivants recensent les capacités déjà implémentées et les pistes à réévaluer selon les besoins de l’API Polars et du moteur Snowflake. Les noms de méthodes cités dans les pistes désignent les cas d’usage et les références de test.

### 4. Transformations relationnelles complémentaires

- [x] **Modification des sorties et déduplication.** Ajouter `with_column` avec ajout ou remplacement et conservation facultative de la position, `rename` simple ou multiple et `distinct`.
- [x] **Unions.** Ajouter `union`, `union_all`, `union_by_name` et `union_all_by_name` avec option de remplissage des colonnes manquantes. Valider l’arité ou les noms quand ils sont connus et déléguer les coercitions de types à Snowflake. [Opérateurs ensemblistes](https://docs.snowflake.com/en/sql-reference/operators-query)
- [x] **Jointures externes.** Étendre les jointures sur condition aux variantes gauche, droite et complète. Propager la nullabilité des côtés complétés par des `NULL`.

- [ ] **Projections et déduplication.** Évaluer les transformations `with_columns`, `drop`, `rename` et `unique` avec ou sans clés, ainsi que les sélecteurs par motif. Évaluer séparément le besoin de projections SQL explicites. Tester conservation de l’ordre des colonnes, remplacements multiples, alias cités, nombre de noms invalide, collisions et déduplication de clés nulles. Pour la déduplication partielle, vérifier la cardinalité et les clés. Si le choix de la ligne conservée est spécifié, ajouter un ordre déterministe au test. [Suite DataFrame Snowpark](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_dataframe_suite.py)
- [ ] **Opérations ensemblistes.** Ajouter `intersect` et `except`/`minus`/`subtract`. Tester ordre de sortie, doublons, `NULL`, coercition de types, arité incompatible, noms ambigus ou cités et binds des deux opérandes. Tester que les projections et filtres respectent la précédence entre opérations et les frontières d’`INTERSECT` et `EXCEPT`. [Opérations ensemblistes](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_dataframe_set_operations_suite.py)
- [ ] **Jointures complémentaires.** Étendre aux jointures semi/anti, cross, `USING`, natural, ASOF et latérales, avec alias ou suffixes explicites en cas de collision. Tester lignes non appariées, nullabilité de chaque côté, colonnes rendues disponibles par semi/anti, clés composites, condition temporelle ASOF, chaîne de jointures et auto-jointure de plusieurs branches du même plan. [Suite des jointures](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_dataframe_join_suite.py)
- [ ] **Agrégats et remodelage.** Ajouter `MIN/MAX/AVG/MEDIAN`, agrégats distincts et ordonnés (`within_group`), `rollup`, `cube`, `grouping_sets`, `pivot` statique puis dynamique, `unpivot`, `describe` et statistiques `corr`, `cov`, quantiles approchés et `crosstab`. Tester `NULL` réels et lignes de total, colonnes pivot absentes, noms générés, alias, options d’`unpivot`, jeux vides et tolérances pour les résultats approchés. [Groupements](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/docs/source/snowpark/grouping.rst) · [Agrégats avancés](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_dataframe_aggregate_suite.py) · [Pivot](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/mock/test_pivot.py)
- [ ] **Données manquantes et échantillonnage.** Évaluer les opérations Polars `drop_nulls`, `fill_null` et `replace`, puis les besoins d’échantillonnage simple ou stratifié et de partitionnement aléatoire. Tester seuils et sous-ensembles, différences `NULL`/NaN, types incompatibles, tailles et fractions limites, poids invalides et strates inconnues. Tester la reproductibilité pour les modes où Snowflake garantit une graine. [Données manquantes et échantillons](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_dataframe_suite.py) · [Paramètres](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_bind_variable.py)
- [ ] **Fenêtres et séries temporelles.** Étudier la traduction de `Expr.over` et des expressions cumulatives ou mobiles vers les partitions, tris et cadres `ROWS` et `RANGE` de Snowflake. Inclure le classement, le décalage, les première et dernière valeurs et les agrégations temporelles selon les besoins. Tester les ex æquo, les bordures de partition, les offsets, les cadres invalides, `IGNORE NULLS` là où applicable et un filtre sur un résultat de fenêtre à travers une sous-requête. [Spécifications de fenêtre](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_window_spec_suite.py) · [Cadres](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_window_frame_suite.py) · [Analytics](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_df_analytics.py)
- [ ] **Fonctions tabulaires.** Ajouter `Session.table_function`, projections avec résultat tabulaire, `join_table_function`, `lateral_join`, `flatten`, générateurs, arguments nommés et alias de sortie. Tester cardinalités zéro/une/plusieurs lignes, résultats de `FLATTEN` avec options et valeurs vides, dépendances corrélées, collisions de noms et paramètres. [API](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/docs/source/snowpark/table_function.rst) · [Tests](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_table_function.py)

### 5. Sources, actions et interopérabilité

- [x] **Composition Python.** `DataFrame.pipe(function, *args, **kwargs)` transmet le DataFrame comme premier argument et renvoie le résultat de l’appel. Tester le chaînage, les arguments positionnels et nommés, le retour d’une valeur Python et la propagation des exceptions. [PR Snowpark Python #4178](https://github.com/snowflakedb/snowpark-python/pull/4178) · [Tests Python](tests/test_dataframe.py)
- [x] **Bindings Arrow.** Exposer les deux actions décrites dans le contrat de session et alimenter PyArrow depuis les lots convertis par Rust. Vérifier les types de retour, la remontée des erreurs et la durée de vie du curseur lorsqu’un itérateur est utilisé après la libération des références Python à la session et au plan.
- [ ] **Sources relationnelles.** Ajouter `sql` avec paramètres en entrée, `range`, `generator` et `create_dataframe` à partir de valeurs Rust, avec schéma explicite puis inférence. Distinguer commandes et requêtes composables. Tester absence d’exécution à la construction, SQL paramétré suivi de transformations, pas positif/négatif/nul, bornes, données vides et nulles, types hétérogènes, largeur incohérente et lots au-delà du seuil d’insertion. [Session](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/docs/source/snowpark/session.rst) · [SQL](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_sql_suite.py) · [Range](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_dataframe_range_suite.py) · [Création de DataFrame](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_dataframe.py)
- [ ] **Inspection.** Définir `explain` et l’inspection du plan compilé autour de `to_sql()`. Tester la disponibilité hors ligne, l’affichage des paramètres à la place des valeurs et la conservation de l’ordre des binds.
- [ ] **Exécution par lots.** Tester l’égalité entre le résultat matérialisé et les lots lus dans l’ordre pour une requête déterministe. Vérifier les lectures concurrentes indépendantes, la libération du curseur après arrêt anticipé, les erreurs après soumission et l’identifiant de requête. Évaluer ensuite l’annulation ou les délais via les capacités du connecteur. [Tests amont de requêtes asynchrones](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_async_job_suite.py)
- [ ] **Interopérabilité Arrow.** Reprendre les cas amont de résultats vides typés, de décimaux, de fuseaux, de valeurs nulles, de champs structurés et de lots multiples. [Tests Arrow](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_df_to_arrow.py)
- [x] **Conversion Polars en Python.** `DataFrame.to_polars(session, *, use_batches=False)` convertit un résultat Arrow. L’extra `snowpark-rs[session]` fournit le backend. Les annotations et les tests couvrent les résultats vides et les lots multiples. [Tests Python](tests/test_session.py)
  - Par défaut, retourner un `polars.DataFrame` avec `pl.from_arrow(self.to_arrow(session))`.
  - Avec `use_batches=True`, retourner un `Iterator[polars.DataFrame]` en appliquant `pl.from_arrow` à chaque lot de `self.to_arrow_batches(session)` au fil de l’itération.

### 6. Écritures, fichiers et durée de vie des objets

- [ ] **Cache Parquet local.** Stocker le cache de chaque table dans `cache_dir / "db.schema.table"`. Le nom pleinement qualifié forme un seul répertoire sous le `cache_dir` configuré via `Session.builder`. Dans ce répertoire, isoler les résultats selon leur plan, leurs paramètres et leur contexte Snowflake, puis écrire les fichiers Parquet dans les sous-répertoires de partition. [Plan du cache](plan_caching.md)
- [ ] **Tables, vues et matérialisation.** Ajouter `save_as_table`, `insert_into`, modes d’écriture, alignement des colonnes par nom ou position, vues temporaires/permanentes et `cache_result`. Ajouter ensuite tables dynamiques et variantes de stockage suivant le besoin. Tester chaque mode initial/sur table existante, ordre des colonnes, visibilité par session, binds acceptés en création et comportement explicite quand Snowflake les refuse, réutilisation du cache, nettoyage après succès/échec et protection des objets permanents. [Écritures](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_dataframe_writer_suite.py) · [Vues](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_view_suite.py) · [Objets temporaires](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_temp_table_cleanup.py)
- [ ] **Mutations.** Ajouter des actions explicites pour `update`, `delete`, `merge`, `drop_table` et leurs résultats typés. Tester filtres, sources jointes et clés ambiguës, clauses `WHEN MATCHED`/`WHEN NOT MATCHED`, ordre des clauses, nombre de lignes modifiées, annulation par rollback quand applicable et absence de rejeu implicite en cas d’issue incertaine. [API Table](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/docs/source/snowpark/table.rst) · [Tests DML](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_update_delete_merge_suite.py)
- [ ] **Lecteurs et `COPY`.** Couvrir CSV, JSON, Parquet, Avro, ORC et XML à partir d’un stage avec options, schéma explicite/inféré et colonnes de métadonnées. Ajouter `COPY INTO` table/emplacement, filtres de fichiers, validation et exports CSV/JSON/Parquet. Tester fichiers multiples, motifs sans correspondance, cas des noms, encodages et options, données malformées, transformations, compteurs et comportement transactionnel. [API I/O](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/docs/source/snowpark/io.rst) · [Reader](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_dataframe_reader_suite.py) · [COPY](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_dataframe_copy_into.py) · [XML](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_xml_infer_schema.py)
- [ ] **Opérations de stage.** Couvrir `put/get` et flux, `list/remove/copy_files`, tables de répertoire et lecture de fichiers référencés. Tester chemins et motifs, fichiers vides, compression, métadonnées, droits insuffisants, transfert interrompu et nettoyage d’un stage temporaire. [Tests fichiers](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_files.py) · [Tests des opérations de stage](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_file_operation_suite.py)

### 7. Session, catalogue, diagnostic et compilateur

- [ ] **Session et transactions.** Ajouter sur une session ouverte les opérations de consultation du contexte et de qualification, de changement de base, schéma, rôle ou warehouse, de gestion des transactions et de configuration des tags et options de requête. Évaluer la création, la clôture et le contexte de session selon les API du connecteur. Tester l’isolation de deux sessions, le changement de rôle, la portée des tags et options, le rollback et les erreurs de session expirée. [Tests session](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_session.py) · [Tests de tags](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/scala/test_query_tag_suite.py)
- [ ] **Catalogue et métadonnées.** Ajouter les recherches, listes, existence et métadonnées des bases, schémas, tables, vues, colonnes, fonctions et procédures, puis mutations explicites du catalogue. Évaluer `semantic_view`, suivi des modifications de table, lineage et variantes SQL/REST en sous-jalons. Tester droits, base et schéma courants, casse, objet absent, pagination le cas échéant et métadonnées cohérentes avec le résultat SQL. [Catalogue](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_catalog.py) · [Semantic views](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_semantic_view.py) · [Table changes](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/unit/test_table_changes_ast.py) · [Lineage](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_lineage.py)
- [ ] **Observabilité.** Ajouter historique ou hooks d’exécution, tags, métriques et diagnostics rattachés au plan et à la requête. Tester erreurs de compilation/soumission/lecture, corrélation par query ID, SQL paramétré dans les traces, filtrage des valeurs sensibles et indépendance de la collecte de métriques par rapport au résultat. Évaluer ensuite traces détaillées, profilage et OpenTelemetry selon le besoin. [Historique](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_query_history.py) · [Trace et erreurs](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_df_trace_actions_and_errors.py) · [OpenTelemetry](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_open_telemetry.py)
- [ ] **Optimisation logique.** Quand la couverture relationnelle est stable, évaluer l’aplatissement de projections, la fusion de filtres, la réutilisation par CTE et le découpage de grands plans. Comparer résultats, doublons, ordre garanti, schéma et paramètres avant et après optimisation. Couvrir les plans en diamant, les alias masqués, `LIMIT`, les agrégats, les fenêtres, les fonctions volatiles, l’ordre des opérations et le nettoyage des objets de découpage. Mesurer la taille du SQL et le nombre de requêtes sur les cas où l’optimisation est activée. [Simplification](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_simplifier_suite.py) · [CTE](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_cte.py) · [Grands plans](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_large_query_breakdown.py)

### 8. Extensions spécialisées

- [ ] **UDF, UDAF, UDTF et procédures.** Définir les contrats appel/enregistrement/déploiement par type de routine et runtime pris en charge, y compris `call_nowait`, dépendances, imports, variantes temporaires/permanentes, vectorisation et profilage. Tester signatures, types de résultats, traitement de `NULL` et d’exceptions, noms qualifiés, surcharges, droits et cycle de vie du stage. Documenter les prérequis Snowflake de chaque runtime avant de planifier son portage. [UDF](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_udf.py) · [UDAF](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_udaf.py) · [UDTF](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_udtf.py) · [Procédures](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_stored_procedure.py) · [Packaging](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_packaging.py)
- [ ] **Sources et traitements spécialisés.** Évaluer les équivalents de DB-API/JDBC avec des connecteurs Rust, `read.documents`, PDF, XPath et XML avancé, ainsi que `DataFrame.ai` et les fonctions Cortex. Pour les sources externes, tester le partitionnement, la conversion de types, la lecture partielle et la reconnexion. Pour les documents, tester les options, le format inconnu, les erreurs d’extraction et le schéma. Pour Cortex, vérifier la compilation, le schéma, les options, les erreurs de rôle et les propriétés stables des résultats. [Sources externes](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/datasource/) · [Documents](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_dataframe_reader_documents.py) · [XPath](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_xpath.py) · [AI](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_dataframe_ai.py)
- [ ] **Écosystème Python et exécution locale.** Décider si `map`, `map_in_pandas`, `apply_in_pandas`, conversions pandas, Snowpark pandas/Modin, secrets côté serveur, mode mock local, échange AST/Protobuf et outils de test publics demandent des équivalents Rust. Les suites d’origine définissent les contrats si une extension est retenue. Pour un mode local, comparer les opérations supportées avec Snowflake sur les mêmes fixtures. Pour des secrets, tester droits, erreurs et absence de valeur dans les traces. Pour un protocole AST, tester sérialisation, version et ordre des instructions. [Modin](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/modin/) · [Mocks](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/mock/) · [Secrets](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_secrets.py) · [AST](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/ast/) · [API de test](https://github.com/snowflakedb/snowpark-python/blob/781939e92343ee4280885eb879c9eaf3ab13e59e/tests/integ/test_testing.py)

### 9. Vocabulaire SQL Snowflake

- [x] **Projections Snowflake.** En Rust, `select` accepte une étoile qualifiée avec `ILIKE` ou `EXCLUDE`, suivis de `REPLACE` et `RENAME`. Le compilateur résout l’ordre, les noms et les types des sorties lorsque le schéma le permet. Les motifs avec une barre oblique inverse sont résolus par Snowflake. [Tests hors ligne](crates/snowflake-ast/tests/star.rs) · [Test Snowflake](tests/snowflake_smoke.rs) · [SELECT](https://docs.snowflake.com/en/sql-reference/sql/select)
- [x] **Filtres d’agrégation et de fenêtre.** `having` filtre les groupes et `qualify` filtre les résultats de fenêtre. Les prédicats peuvent référencer les alias de la projection. Les tests couvrent la composition avec `GROUP BY`, les fenêtres, `DISTINCT`, le tri et la limite. [Tests SQL](crates/snowflake-ast/tests/post_filters.rs) · [HAVING](https://docs.snowflake.com/en/sql-reference/constructs/having) · [QUALIFY](https://docs.snowflake.com/en/sql-reference/constructs/qualify)
- [x] **Conversions et données semi-structurées.** L’AST accepte `TRY_CAST` et le namespace `.snowflake` expose `TRY_TO_NUMBER`, `TRY_PARSE_JSON`, `TYPEOF`, `GET_PATH`, `ARRAY_CONSTRUCT`, `OBJECT_CONSTRUCT_KEEP_NULL` et `IS_NULL_VALUE`. Les tests couvrent la différence entre `NULL` SQL, `null` JSON et une clé absente. [Conversions](https://docs.snowflake.com/en/sql-reference/functions/try_cast) · [Fonctions semi-structurées](https://docs.snowflake.com/en/sql-reference/functions-semistructured)
- [ ] **Parcours hiérarchiques.** Ajouter `WITH RECURSIVE`, puis `CONNECT BY` avec `START WITH`, `PRIOR` et `LEVEL`. Tester les portées d’alias, les racines multiples, l’ordre des binds et les bornes de parcours. [WITH](https://docs.snowflake.com/en/sql-reference/constructs/with) · [CONNECT BY](https://docs.snowflake.com/en/sql-reference/constructs/connect-by)
- [ ] **Historique et séries temporelles.** Ajouter `AT`/`BEFORE` et `CHANGES` pour les lectures historiques, puis `RESAMPLE` et `MATCH_RECOGNIZE`. Tester la rétention, les bornes temporelles, les partitions, les lignes générées et les résultats avec ou sans correspondance. [AT et BEFORE](https://docs.snowflake.com/en/sql-reference/constructs/at-before) · [CHANGES](https://docs.snowflake.com/en/sql-reference/constructs/changes) · [RESAMPLE](https://docs.snowflake.com/en/sql-reference/constructs/resample) · [MATCH_RECOGNIZE](https://docs.snowflake.com/en/sql-reference/constructs/match_recognize)

## Critères de validation par fonctionnalité

- **Compilation et API Rust.** Exécuter `cargo test --locked --no-default-features` sur les membres Rust par défaut pour la compilation hors ligne, puis `cargo test --locked -p snowpark-rs --features session` pour l’adaptateur et Arrow. Vérifier dans le graphe Cargo des dépendances normales que le connecteur et Arrow sont absents de la compilation de base. Tester le SQL, les binds et les erreurs locales dans `snowflake-ast`.
- **Bindings et installation Python.** Tester la compilation hors ligne avec Polars dans un environnement sans PyArrow ni backend de session. Tester l’exécution Arrow et la conversion Polars dans un environnement avec l’extra `session`. Vérifier les signatures et les formes d’appel avec `pytest`. Le hook Codex `Stop` lance `uvx ruff check` et `uvx ty check` à la fin du tour, après approbation du hook dans Codex. [Configuration du hook](.codex/hooks.json)
- **Sur Snowflake.** Conserver les tests à connexion réelle ignorés par défaut. Les lancer avec un profil de test et `cargo test --locked -p snowpark-rs --features session --test snowflake_smoke -- --ignored`. Vérifier que le même DataFrame peut s’exécuter avec deux sessions explicites ayant des contextes différents. Vérifier les lignes, types, noms et nullabilité, y compris sur un résultat vide. Comparer des multisets sans ordre contractuel et des séquences avec clés de tri déterministes quand l’ordre fait partie de l’API.
- **Comparaison avec Polars.** Évaluer les mêmes expressions sur une fixture Polars locale et sur la table Snowflake correspondante. Comparer valeurs, schéma, ordre lorsqu’il est garanti et comportement sur les cas limites. Les suites Snowpark citées fournissent des scénarios supplémentaires pour le moteur SQL. Cocher une tâche quand le code et les tests de comportement, d’erreur et de composition correspondants sont présents.
