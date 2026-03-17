# JMESPath Path Expressions vs SQL SELECT Statements

A theoretical analysis of the correspondence between JMESPath and SQL,
with attention to the two-level computation model used in blobapi/blobhttp
where JMESPath expressions are stored as data in adapter tables and
evaluated at runtime by generic SQL.

---

## 1. The Overlap — What maps between JMESPath and SQL

### 1.1 Projection (SELECT col1, col2)

**SQL:**
```sql
SELECT date, temperature FROM weather_observations;
```

**JMESPath:**
```
observations[].{date: date, temperature: temperature}
```

The JMESPath multi-select hash `{key: expr, ...}` is the direct analog of
SELECT with column aliases. The `[]` projection operator applies the hash to
every element in the array, like SELECT applies to every row.

Concrete example from `weather.yaml`:
```
days[].{date: datetime, temperature: temp, unit_of_measure: 'C'}
```
is equivalent to:
```sql
SELECT datetime AS date, temp AS temperature, 'C' AS unit_of_measure
FROM days;
```

### 1.2 Filtering (WHERE predicate)

**SQL:**
```sql
SELECT * FROM people WHERE age > 20;
```

**JMESPath:**
```
people[?age > `20`]
```

The filter expression `[?predicate]` maps directly to WHERE. JMESPath
supports `==`, `!=`, `<`, `<=`, `>`, `>=` and logical `&&`, `||`, `!`.
The backtick syntax (`` `20` ``) denotes a literal JSON value (number),
distinguishing it from a string.

Composed with projection:
```
people[?age > `20`].{name: name, age: age}
```
is:
```sql
SELECT name, age FROM people WHERE age > 20;
```

### 1.3 Nested field access (JOIN-like navigation)

**SQL (requires JOIN):**
```sql
SELECT o.order_id, c.name
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.customer_id;
```

**JMESPath (if nested):**
```
orders[].{order_id: order_id, customer_name: customer.name}
```

JMESPath navigates nested structures with dot notation. This only works
when the data is already nested (denormalized). It cannot combine two
independent flat collections — that requires a JOIN, which JMESPath lacks
entirely. The correspondence holds only for pre-joined / document-model data.

### 1.4 Flattening (UNNEST / LATERAL)

**SQL:**
```sql
SELECT o.order_id, item.*
FROM orders AS o, UNNEST(o.items) AS item;
```

**JMESPath:**
```
orders[].items[]
```

The flatten operator `[]` on an already-projected array-of-arrays merges
one level of nesting. This is exactly UNNEST / LATERAL JOIN. Repeated
application (`[][]`) flattens deeper, like recursive UNNEST.

Note: JMESPath distinguishes `[*]` (iterate without flatten) from `[]`
(iterate and flatten). The flatten form is the one that corresponds to
UNNEST.

### 1.5 Sorting (ORDER BY)

**SQL:**
```sql
SELECT * FROM people ORDER BY age DESC;
```

**JMESPath:**
```
people | sort_by(@, &age) | reverse(@)
```

JMESPath has `sort(@)` for arrays of scalars and `sort_by(array, &key)`
for arrays of objects. There is no ASC/DESC — you reverse the result
with `reverse(@)`. The pipe `|` feeds the result of one expression into
the next.

Multi-column sort is not directly supported. You would need a custom
function or a compound key expression.

### 1.6 Slicing (LIMIT / OFFSET)

**SQL:**
```sql
SELECT * FROM items ORDER BY price LIMIT 5 OFFSET 10;
```

**JMESPath:**
```
items | sort_by(@, &price) | [10:15]
```

Array slicing `[start:stop:step]` maps to OFFSET/LIMIT. Python-style
negative indices are supported: `[-3:]` gives the last 3 elements.
JMESPath slicing is zero-indexed.

### 1.7 Computed columns / expressions

**SQL:**
```sql
SELECT price * quantity AS total FROM line_items;
```

**JMESPath (standard):**
```
-- Cannot do arithmetic. No multiplication operator.
```

**JMESPath (jsoncons extended):**
```
line_items[].{total: multiply(price, quantity)}
-- Only if multiply() is registered as a custom function.
```

Standard JMESPath has no arithmetic operators. It can concatenate strings
via `join()` and compute `length()`, `avg()`, `sum()` etc. on arrays,
but it cannot compute new scalar values from existing scalar fields. This
is a significant gap — SQL's expression language is far richer. The
jsoncons custom function mechanism can close this gap function by function,
but it never reaches SQL's generality.

### 1.8 Multiselect (reshaping — SELECT with aliases)

**SQL:**
```sql
SELECT lat AS latitude, lng AS longitude, 'geocodio' AS source
FROM geocode_result;
```

**JMESPath:**
```
{latitude: lat, longitude: lng, source: 'geocodio'}
```

Multi-select hash is the JMESPath primitive for reshaping. It constructs
a new object with arbitrary keys mapped to arbitrary sub-expressions. This
is arguably more natural than SQL's aliasing because the output shape is
explicit in the expression.

Multi-select list `[expr1, expr2]` produces an array (like SELECT into an
unnamed tuple):
```
[lat, lng]  -->  [42.3601, -71.0589]
```

### 1.9 Aggregation (GROUP BY + aggregate functions)

**SQL:**
```sql
SELECT department, AVG(salary) AS avg_salary
FROM employees
GROUP BY department;
```

**JMESPath:**
```
-- No GROUP BY. Can only aggregate a whole array:
avg(employees[].salary)        -- scalar result, no grouping
length(employees)              -- count
sum(employees[].salary)        -- total
min(employees[].salary)        -- minimum
max(employees[].salary)        -- maximum
```

JMESPath has aggregate functions (`avg`, `sum`, `min`, `max`, `length`)
but they operate on a single array and produce a single scalar. There is
no GROUP BY — you cannot partition an array into groups and aggregate each
group independently. To simulate it, you would need to know the groups in
advance and filter each one separately:

```
{
  engineering: avg(employees[?dept=='engineering'].salary),
  sales:       avg(employees[?dept=='sales'].salary)
}
```

This requires enumerating all groups statically in the expression. It is
not data-driven grouping.

### 1.10 Conditional logic (CASE WHEN)

**SQL:**
```sql
SELECT name,
       CASE WHEN score >= 90 THEN 'A'
            WHEN score >= 80 THEN 'B'
            ELSE 'C' END AS grade
FROM students;
```

**JMESPath:**
```
-- No conditional/ternary operator in standard JMESPath.
-- NOT possible to produce different output values for the same
-- object depending on a condition.
```

This is a real gap. JMESPath has no if/then/else, no CASE WHEN, no
ternary operator. Filters can select which objects pass, but they cannot
produce different output values for the same object depending on a
condition.

---

## 2. The Gap — What SQL can do that JMESPath fundamentally cannot

### 2.1 Joins (combining two independent data sources)

**SQL:**
```sql
SELECT w.date, w.temperature, l.city
FROM weather AS w
JOIN locations AS l ON w.location_id = l.location_id;
```

**JMESPath:** impossible.

**Why:** JMESPath operates on a single JSON document. Its data model is a
tree (or DAG if `$ref` pointers are resolved). There is no concept of a
"second input" — the expression evaluates against one root value. A join
requires two independent collections with a matching predicate. This is
not a missing feature; it is a fundamental constraint of the
single-document evaluation model.

Theoretical note: You could embed both collections in one document
(`{weather: [...], locations: [...]}`) and then simulate a nested-loop
join via nested projections. But standard JMESPath cannot correlate across
projection scopes — the inner projection cannot reference a variable bound
in the outer projection. There is no variable binding.

### 2.2 Self-joins

Same limitation as joins. Even though the data is in one document,
JMESPath cannot correlate one element of an array with another element of
the same array based on a matching condition. There is no cross-product
or equi-join operator.

**Fundamental vs missing:** Fundamental. Adding cross-references between
array elements would require variable binding (like XPath's
`for $x in ... return ...`), which changes the language's computational
model.

### 2.3 Window functions (ROW_NUMBER, LAG, LEAD, running totals)

**SQL:**
```sql
SELECT date, temperature,
       temperature - LAG(temperature) OVER (ORDER BY date) AS delta
FROM weather;
```

**JMESPath:** impossible.

**Why:** Window functions require positional awareness — "what is the value
of the previous/next element relative to the current one?" JMESPath
projections are element-wise: each element is processed independently
with no access to its neighbors or its index position.

**Fundamental vs missing:** Partially fundamental. The element-wise
projection model precludes neighbor access. However, a custom function
could receive the whole array and compute windowed results — e.g.,
`lag(array, &key, offset)`. The jsoncons custom function mechanism makes
this possible, but the result would be an array-level function, not a
per-element expression. It changes the programming model from declarative
to procedural-via-function-calls.

### 2.4 Recursive CTEs

**JMESPath:** impossible. JMESPath has no recursion, no iteration, no
fixpoint. It is strictly non-recursive: the expression tree is evaluated
top-down in a single pass. There is no way to express "keep doing this
until no more results."

**Fundamental:** Yes. JMESPath is intentionally not Turing-complete.
Adding recursion would change it from a query/projection language into a
general programming language, which is explicitly not a design goal.

### 2.5 Correlated subqueries

**JMESPath:** impossible in general. Correlation requires referencing an
outer binding inside an inner expression. JMESPath's scoping rules reset
the current node at each projection boundary. The `@` reference always
points to the current element, never to an element in an enclosing scope.

Partial workaround: If the data is already nested (orders inside
employees), then `employees[].{name: name, order_count: length(orders)}`
works. But this requires the document structure to mirror the query
structure — the query cannot impose a new correlation.

### 2.6 Set operations (UNION, INTERSECT, EXCEPT)

No set operations in JMESPath. You can concatenate arrays with a custom
function, but duplicate elimination (UNION vs UNION ALL), intersection,
and difference are not available.

**Fundamental vs missing:** Missing feature. Set operations could be
added as built-in functions without changing the language model. The spec
just does not include them.

### 2.7 Stateful / cumulative aggregation

Same limitation as window functions. Accumulator-style computation
requires state carried between elements. JMESPath projections are
stateless.

### 2.8 Multi-table operations

SQL's FROM clause can name multiple tables. JMESPath's input is always a
single document. Any operation that requires independent data sources
(joins, unions, cross-references between tables) is outside JMESPath's
model.

### 2.9 Mutation (UPDATE / DELETE)

JMESPath is purely functional / read-only. It produces a new JSON value;
it never modifies the input. This is by design. Mutation belongs to the
host language or database.

### 2.10 Grouping with aggregation (GROUP BY + HAVING)

No JMESPath equivalent. The combination of GROUP BY + aggregate + HAVING
— which is the backbone of analytical SQL — has no JMESPath counterpart.

---

## 3. The Gap (other direction) — What JMESPath does that SQL cannot

### 3.1 Deep nested traversal

**JMESPath:**
```
spec.paths.*.get.responses."200".content."application/json".schema
```

SQL can do this with `json_extract` / `JSON_QUERY`, but the path is
opaque to the query planner. JMESPath treats every level as a first-class
navigation step with wildcard support. SQL's JSON path support varies by
dialect and is generally less expressive for multi-level wildcard
traversal.

The critical difference: JMESPath wildcard `*` projects over all keys at
a level, producing an array of results. In SQL, you would need
`OPENJSON` / `json_each` / `json_tree` and explicit `CROSS APPLY` /
`LATERAL JOIN` for each level of wildcarding.

### 3.2 Wildcard projections across heterogeneous structures

**JMESPath:**
```
*.tags[0]
```

Applied to an object with varying shapes per key, this collects the first
tag from every top-level value, regardless of what other fields those
values contain. SQL would need to know the keys in advance (or use
`json_each` + lateral).

JMESPath is schema-oblivious: a path step that does not exist returns
null and the projection silently skips it. SQL's `json_extract` also
returns null, but composing multiple levels of schema-oblivious access is
syntactically painful in SQL and natural in JMESPath.

### 3.3 Pipe expressions (function composition)

**JMESPath:**
```
locations[?state == 'WA'].name | sort(@) | {WashingtonCities: join(', ', @)}
```

This is a three-stage pipeline: filter, sort, reshape. Each `|` feeds
the output of the left side as the input to the right side. The result
of the full expression is a new JSON document.

**SQL equivalent:**
```sql
WITH FILTERED AS (
    SELECT name FROM locations WHERE state = 'WA'
),
SORTED AS (
    SELECT name FROM FILTERED ORDER BY name
)
SELECT json_object('WashingtonCities',
         group_concat(name, ', ')) FROM SORTED;
```

The SQL is more verbose but more powerful (each CTE can join, group,
window). The JMESPath pipe is more concise for linear transformations but
cannot branch or merge — it is strictly sequential.

Pipe composition is where JMESPath achieves its elegance as an adapter
language: the output of one API call, reshaped by a pipe, becomes the
input to the next. This is the Unix philosophy applied to JSON documents.

### 3.4 Multi-select hash/list (reshaping without schema declaration)

**JMESPath:**
```
{url: join('/', [base_url, 'timeline', zip, start_date, end_date]),
 params: {key: api_key, unitGroup: 'metric'}}
```

This constructs a nested JSON object in a single expression. The JMESPath
version reads as a JSON template with holes, while the SQL equivalent
reads as function calls building a string. For the specific use case of
"construct a JSON request from a context object," JMESPath is more
natural.

### 3.5 Operating on schema-less / polymorphic data

JMESPath does not require — or even support — type declarations. Every
value is just JSON. A single expression can process documents of varying
shapes, silently returning null for missing paths.

SQL requires a schema (or at least a `json_extract` with a known path).
Processing a table of JSON documents with varying schemas in SQL requires
either sparse columns, `json_extract` per field, or `json_each` to shred
into rows.

JMESPath's strength is precisely this: it is a query language for a
schema-less data model. SQL is a query language for a schema-ful data
model. They meet in the middle when SQL has JSON columns and JMESPath
results are consumed as relational rows.

---

## 4. The Code-as-Data Pattern

The blobapi/blobhttp architecture stores JMESPath expressions in adapter
tables:

```
llm_adapter.response_jmespath    -- LLM response → common schema
api_adapter.call_jmespath        -- context → {url, params}
api_adapter.response_jmespath    -- API response → common schema
```

A generic SQL query reads these expressions and passes them to
`jmespath_search()` — a C++ scalar function (jsoncons) exposed via
blobtemplates into DuckDB and SQLite:

```sql
SELECT jmespath_search(response_body, a.response_jmespath)
FROM http_response AS r
JOIN api_adapter AS a ON r.provider = a.provider ...;
```

The **same SQL query** produces **different outputs** depending on the
JMESPath expression stored in the table. INSERT/UPDATE on the adapter
table changes runtime behavior without recompilation, redeployment, or
even query modification.

### 4.1 Comparison to Lisp homoiconicity

In Lisp, code and data share the same representation (s-expressions).
`(eval (read "(+ 1 2)"))` reads a string as data, then evaluates it as
code. The crucial property: the language can inspect, transform, and
generate its own programs.

The blobapi pattern has the same structure:

- JMESPath expressions are data (stored in a TEXT column)
- They are read by SQL
- They are evaluated by `jmespath_search()` against a JSON document
- The SQL layer can inspect them (they are just strings)
- The SQL layer can construct them (string concatenation, templates)

**Key difference:** Lisp's homoiconicity is self-referential — Lisp code
generates Lisp code that generates Lisp code. The blobapi pattern is
heterogeneous: SQL code manipulates JMESPath code that transforms JSON
data. There are **three levels**, not two:

| Level | Language | Role |
|---|---|---|
| 0 | SQL | Orchestrator — reads expressions, calls functions |
| 1 | JMESPath | Transformer — reshapes JSON |
| 2 | JSON | Data — API responses, request parameters |

In Lisp, levels 0 and 1 collapse into one language. In blobapi, keeping
them separate is the point: SQL handles what SQL is good at (joining,
aggregating, persisting), JMESPath handles what it is good at (navigating
and reshaping JSON trees), and JSON is the lingua franca between
functions.

### 4.2 Comparison to SQL dynamic SQL / EXEC

```sql
DECLARE @sql NVARCHAR(MAX) = N'SELECT * FROM ' + @table_name;
EXEC sp_executesql @sql;
```

This is code-as-data within a single language: SQL generates SQL and
evaluates it. The risks are well-known: SQL injection, opaque query plans,
difficulty debugging.

The JMESPath pattern is **safer** because the evaluated language is
strictly less powerful:

- JMESPath cannot mutate data (no INSERT/UPDATE/DELETE)
- JMESPath cannot access the database (no FROM clause, no table refs)
- JMESPath cannot call external functions (unless explicitly registered)
- JMESPath is not Turing-complete (no unbounded loops)

A malicious or buggy JMESPath expression can produce wrong output but
cannot corrupt state. The blast radius is bounded. This is the same
safety property that makes SQL's CHECK constraints safe: the expression
language is deliberately limited.

### 4.3 Comparison to stored procedures with parameterized queries

A stored procedure with parameters parameterizes the **values** that flow
through a fixed query structure. The blobapi pattern parameterizes the
**transformation itself**. The query structure is fixed, but the reshaping
logic (which fields to extract, how to nest them, what to call them)
varies per row in the adapter table.

This is a deeper form of parameterization: not "what data to fetch" but
"how to interpret what was fetched."

### 4.4 The expression problem (PL theory)

The expression problem asks: can you add both new data variants AND new
operations without modifying existing code?

In the blobapi pattern:

- **New data variant** (new API provider): INSERT a row into the adapter
  table. No code changes. The generic SQL handles it.
- **New operation** (new output schema): UPDATE the `response_jmespath`.
  No code changes. The generic SQL handles it.
- **New kind of transformation** (e.g., `zip_arrays`): Register a new
  custom function in the C++ layer. This **does** require code changes
  and recompilation.

The pattern solves the expression problem for the common case (new
providers, new schemas) and falls back to code changes only when the
transformation vocabulary itself needs extension.

### 4.5 Advantages of two-level computation

- **Separation of concerns:** SQL handles orchestration (sequencing,
  joining, persisting). JMESPath handles shape (projecting, filtering,
  renaming). Each language is used where it excels.

- **Late binding:** The transformation is determined at query time, not
  compile time. A new adapter row takes effect immediately. This is the
  database equivalent of dynamic dispatch.

- **Auditability:** The JMESPath expression is a string in a temporal
  table. You can query the history: "what transformation was in effect on
  2024-03-15?" — just join on `sys_from`/`sys_to`. Try doing that with
  compiled code.

- **Composability:** Because `jmespath_search()` is a scalar function
  returning JSON, its output composes with any other JSON-consuming
  function in the same SELECT. No special integration needed.

### 4.6 Risks

- **Injection:** If JMESPath expressions are constructed from user input
  via string concatenation, malicious expressions could extract unexpected
  data. However, since JMESPath is read-only and operates on a single
  document, the impact is limited to information disclosure within that
  document — no database mutation, no privilege escalation.

- **Debugging:** When a pipeline produces wrong output, you must
  determine whether the bug is in the SQL (wrong input document passed to
  `jmespath_search`), in the JMESPath (wrong expression), or in the data
  (API returned unexpected shape). Three levels means three places to
  look. Tooling for JMESPath debugging is sparse compared to SQL EXPLAIN.

- **Composability limits:** Two `jmespath_search()` calls in a CTE chain
  can each reshape JSON, but they cannot share intermediate results
  except through the JSON document. If step 2 needs a value from step 1
  AND a value from the original input, the original input must be
  threaded through — it is not in scope. SQL handles this naturally (CTEs
  can reference each other); JMESPath cannot.

- **Type erasure:** Everything is JSON text between steps. The SQL engine
  does not know or check the schema of intermediate JSON results. A typo
  in a JMESPath field name silently produces null instead of raising an
  error. Static type checking is impossible across the SQL/JMESPath
  boundary.

---

## 5. The Composition Model

The blobapi/blobhttp pipeline composes as:

```
SQL CTE  →  scalar_fn(json)  →  JMESPath reshapes  →  scalar_fn(json)  →  ...
```

Concretely:
```sql
WITH CREDS AS (
    SELECT jmespath_search(vault_response, creds_jmespath) AS creds
),
REQUEST AS (
    SELECT jmespath_search(
        json_object('base_url', a.base_url, 'api_key',
                    json_extract_string(creds, '$.api_key'), ...),
        a.call_jmespath
    ) AS req FROM CREDS, api_adapter AS a ...
),
RESPONSE AS (
    SELECT bh_http_get(
        json_extract_string(req, '$.url'),
        params := json_extract(req, '$.params')
    ) AS resp FROM REQUEST
),
NORMALIZED AS (
    SELECT jmespath_search(resp.response_body, a.response_jmespath) AS data
    FROM RESPONSE, api_adapter AS a ...
)
SELECT * FROM NORMALIZED;
```

Each CTE is one stage. JSON flows between stages. JMESPath reshapes at
each boundary.

### 5.1 Comparison to Unix pipes

```
cat data.json | jq '.locations[] | select(.state=="WA")' | sort | head -5
```

Similarities:
- Linear dataflow: each stage consumes the output of the previous
- Uniform interface: Unix uses byte streams, blobapi uses JSON text
- Composability: any stage can be replaced without affecting others

Differences:
- Unix pipes are streaming (unbounded); CTE chains are materialized
  (bounded)
- Unix pipes have **one** channel; CTEs can reference **any** earlier CTE.
  The CTE DAG is richer than a linear pipe.
- Unix pipe stages run concurrently (OS scheduler); CTE stages may or may
  not (query optimizer decides)

### 5.2 Comparison to functional programming (map/filter/reduce)

The JMESPath projection `items[].{...}` is **map**.
The JMESPath filter `items[?pred]` is **filter**.
The JMESPath aggregates `sum(items[].x)` are **reduce** (fold).
The JMESPath pipe `expr1 | expr2` is **function composition** (>>).

The full pipeline is:
```
compose(
  map(call_jmespath),     -- reshape context → request
  bh_http_get,            -- effectful: side-effecting I/O
  map(response_jmespath)  -- reshape response → common schema
)
```

The `bh_http_get` in the middle is the impure part. In a pure FP model, it
would be wrapped in IO or an effect monad. In SQL, effects happen inside
scalar functions and the engine is blissfully unaware — there is no
effect tracking. This is both a strength (simplicity) and a weakness (no
retry, no backpressure, no error channel other than null/exception).

### 5.3 What breaks: JOINing two intermediate results

Suppose you need weather from TWO providers and want to compare them:

```sql
WITH VC_DATA AS (
    SELECT jmespath_search(vc_response, vc_adapter.response_jmespath) AS data
),
OM_DATA AS (
    SELECT jmespath_search(om_response, om_adapter.response_jmespath) AS data
)
-- Now you need to JOIN these two JSON arrays by date.
-- JMESPath cannot help here. You must shred to rows in SQL:
SELECT vc.date, vc.temperature AS vc_temp, om.temperature AS om_temp
FROM (SELECT unnest(vc_data.data) AS vc FROM VC_DATA)
JOIN (SELECT unnest(om_data.data) AS om FROM OM_DATA)
  ON vc.date = om.date;
```

The JMESPath expressions normalize each response independently, but
**combining** the normalized results requires SQL. This is the boundary:
JMESPath transforms single documents; SQL combines multiple results.

The boundary is clean: JMESPath handles the vertical (single-source
reshaping); SQL handles the horizontal (multi-source combination).

### 5.4 Category theory perspective

If we squint:

- **JSON values** form a category (objects are JSON values, morphisms are
  JMESPath expressions — composition is pipe `|`, identity is `@`)

- **Relational tuples** form a category (objects are relation schemas,
  morphisms are SQL queries — composition is CTE chaining, identity is
  `SELECT *`)

- The `json_extract` / `json_object` functions are **functors** between
  these categories: they map relational values to JSON and back.

- `jmespath_search()` is a **natural transformation**: for any SQL context
  (any row), it applies a JMESPath morphism to the JSON component of that
  row, preserving the relational structure around it.

More precisely, consider the functor F: Rel → JSON that extracts the JSON
column from a relational row. And the functor G: JSON → Rel that wraps a
JSON value back into a row. Then `jmespath_search(col, expr)` is a
natural transformation η: F ⇒ F, parameterized by the JMESPath
expression. It transforms the JSON component while the SQL CTE handles
the relational structure.

The CTE chain is then:

```
Rel -F→ JSON -η₁→ JSON -G→ Rel -F→ JSON -η₂→ JSON -G→ Rel
```

Where η₁ = `call_jmespath` and η₂ = `response_jmespath`. The alternation
between Rel and JSON is the alternation between SQL and JMESPath
evaluation. `bh_http_get` breaks the pattern because it is an effect (it
leaves the category of pure transformations).

Is this a monad? Not quite. A monad would require JSON-in-JSON nesting
(`join: M(M(A)) → M(A)`) to have a consistent flatten operation.
JMESPath's `[]` (flatten) does serve this role for nested arrays, but the
Rel-JSON-Rel sandwich is better described as an **adjunction** between
the relational and JSON worlds, with the scalar functions as the unit
and counit.

---

## 6. Concrete Limits — What will NOT work

### 6.1 Data engineering examples

- **Cannot:** Deduplicate API responses across multiple calls.
  If two API calls return overlapping date ranges, JMESPath cannot detect
  or remove duplicates across the two response documents. Must use SQL:
  `SELECT DISTINCT` or `GROUP BY` after unnesting.

- **Cannot:** Incrementally merge new API data with existing local data.
  JMESPath operates on a single document. It cannot compare the API
  response against what is already in the database and compute a delta.
  Must use SQL `MERGE` / `INSERT ... ON CONFLICT`.

- **Cannot:** Validate referential integrity across API responses.
  "Does every `location_id` in the weather response exist in the
  locations table?" requires a join. JMESPath cannot do this.

- **Cannot:** Compute time-series aggregations across responses.
  Monthly averages, year-over-year comparisons, moving averages — these
  require window functions or GROUP BY on data spanning multiple API
  calls. Must use SQL.

### 6.2 LLM-driven data classification examples

- **Cannot:** Compare classification results across multiple columns.
  "Which columns in this table were classified as dimensions?" requires
  scanning the extended properties for all columns and filtering — a SQL
  query against `sys.extended_properties`, not a JMESPath expression.

- **Cannot:** Correlate LLM classification with histogram statistics.
  "Does the LLM's dimension/measure label agree with the cardinality
  ratio?" requires joining the classification result (from the LLM
  response, extractable via JMESPath) with the histogram statistics
  (stored in DuckDB/SQL Server tables). JMESPath extracts the label; SQL
  does the correlation.

- **Cannot:** Build a confusion matrix from LLM vs rule-based classifiers.
  This requires aggregation (`GROUP BY predicted_label, actual_label`
  with `COUNT`) across all columns — pure SQL.

- **Cannot:** Route to different LLMs based on column metadata.
  "If data type is varchar, use model A; if numeric, use model B."
  JMESPath cannot branch on conditions to select different downstream
  operations. SQL's `CASE WHEN` + different scalar function calls handles
  this.

### 6.3 Where to draw the boundary

**JMESPath should handle:**
- Extracting fields from a single API response
- Renaming/reshaping to a common schema
- Constructing request parameters from a context object
- Filtering elements within a single response
- Transposing columnar to row-oriented (via `zip_arrays`)

**SQL should handle:**
- Combining results from multiple API calls (JOINs)
- Aggregation and grouping
- Deduplication
- Comparison with local data
- Conditional routing (`CASE WHEN` → different function calls)
- Persistence (`INSERT`/`UPDATE`/`MERGE`)
- Temporal queries (`sys_from`/`sys_to` joins)

**The boundary rule:** if the operation has **one input document** and
**one output document**, it can be JMESPath. If it has **many inputs** or
requires **state across inputs**, it must be SQL.

---

## 7. The jsoncons JMESPath dialect

The project uses jsoncons (C++ library) which provides a fully compliant
JMESPath implementation plus a custom function extension mechanism. Four
custom functions are registered in blobtemplates:

### 7.1 `zip_arrays(obj)` — columnar to row-oriented

**Input:**
```json
{"time": ["2024-01-01", "2024-01-02"], "temp": [5.2, 6.1]}
```

**Output:**
```json
[{"time": "2024-01-01", "temp": 5.2}, {"time": "2024-01-02", "temp": 6.1}]
```

This is the most important extension. It closes the gap for APIs (like
Open-Meteo) that return parallel arrays instead of arrays of objects.
Without it, the `response_jmespath` for such APIs would need SQL-side
unnesting.

**Expressiveness impact:** HIGH. This transforms a class of responses
that are otherwise impossible to handle in JMESPath (since standard
JMESPath cannot correlate across parallel arrays — it is a join, which
is exactly the gap identified in section 2.1). `zip_arrays` encapsulates
that join inside a function, hiding it from the expression language.

### 7.2 `unzip_arrays(arr)` — row-oriented to columnar

Inverse of `zip_arrays`. Useful when an API expects columnar input but
the data is stored row-oriented.

**Expressiveness impact:** MODERATE.

### 7.3 `to_entries(obj)` — object to key/value pairs

**Input:**
```json
{"US": "United States", "GB": "United Kingdom"}
```

**Output:**
```json
[{"key": "US", "value": "United States"}, {"key": "GB", "value": "United Kingdom"}]
```

This lets JMESPath iterate over object keys — something not possible in
standard JMESPath (wildcard `*` gives values but loses the keys).
`to_entries` preserves both, enabling the common pattern:
```
to_entries(obj)[].{code: key, name: value}
```

**Expressiveness impact:** MODERATE-HIGH. Closes the "iterate with keys"
gap.

### 7.4 `from_entries(arr)` — key/value pairs to object

Inverse of `to_entries`. Constructs an object from an array of
`{key, value}` pairs. Enables constructing objects with dynamic keys
(which multi-select hash cannot do — its keys must be string literals).

**Expressiveness impact:** MODERATE.

### 7.5 Do these extensions close the gaps?

`zip_arrays` partially closes the JOIN gap (section 2.1) — but only for
the specific case of parallel arrays within a single object. It does NOT
enable joining two independent documents.

`to_entries`/`from_entries` close the "iterate over keys" gap, which is
a subset of the wildcard-with-metadata gap (section 3.2).

**The fundamental gaps remain open:**

- Multi-document joins: still impossible
- GROUP BY / aggregation by dynamic groups: still impossible
- Window functions / positional awareness: still impossible
- Conditional logic (CASE WHEN): still impossible
- Recursion: still impossible
- Mutation: still impossible (and should remain so)

The extensions are surgical: they close exactly the gaps that arise in
the specific use case (API response normalization) without attempting to
make JMESPath into a general-purpose language. This is the right design.
The boundary between JMESPath and SQL should be maintained, not eroded.

---

## Summary

| Operation | SQL | JMESPath | Notes |
|---|---|---|---|
| Projection | YES | YES | Multi-select hash = SELECT |
| Filtering | YES | YES | `[?pred]` = WHERE |
| Nested access | YES | YES | Dot notation (pre-joined) |
| Flatten/UNNEST | YES | YES | `[]` operator |
| Sorting | YES | YES | `sort_by()` + `reverse()` |
| Slicing | YES | YES | `[start:stop]` |
| Computed expressions | YES | PARTIAL | No arithmetic operators |
| Reshaping/aliasing | YES | YES | JMESPath more natural |
| Aggregation (whole) | YES | YES | `sum`/`avg`/`min`/`max`/`length` |
| Aggregation (grouped) | YES | NO | No GROUP BY |
| Conditional logic | YES | NO | No CASE/IF |
| Joins | YES | NO | Fundamental: single-doc |
| Self-joins | YES | NO | Fundamental: no correlation |
| Window functions | YES | NO | Fundamental: stateless projection |
| Recursive CTE | YES | NO | Fundamental: no recursion |
| Set operations | YES | NO | Missing feature |
| Mutation | YES | NO | By design |
| Deep wildcard traversal | POOR | YES | JMESPath excels |
| Schema-less navigation | POOR | YES | JMESPath excels |
| Pipe composition | CTE | YES | Both work; JMESPath terser |
| Parallel-array transpose | UGLY | YES* | *via `zip_arrays` extension |
| Dynamic key iteration | YES | YES* | *via `to_entries` extension |
