# DBCollections.jl

> [!IMPORTANT]
> Imagine if you could use the exact same code, the same functions to operate on regular Julia collections and on SQL databases... DBCollections.jl is the only Julia package that achieves that!

DBCollections.jl supports a large set of operations, `map`/`filter`/`group`/`first`/... . Planned: `join`.
Works with any database supported by FunSQL.jl.

From the implementation point of view, defining characteristics of DBCollections.jl are:
- Ease of maintenance: \
It's a very thin layer converting Julia functions to SQL code.\
For comparison, number of code lines (excluding tests) in packages with roughly the same goals:
  - **DBCollections.jl**: ~200 *(although still growing)*
  - dplython: ~700
  - datar: ~2300
  - TidierDB.jl: ~4000
  - Blaze: ~10000
  - Ibis: ~43000

- No magic, no macros defined, no special parsing/interpolation rules: \
The API consists of regular functions. This also makes DBCollections.jl play nicely with convenience macros from other packages (eg, Accessors and DataPipes).
