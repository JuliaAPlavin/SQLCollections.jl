# DBCollections.jl

> [!IMPORTANT]
> Imagine if you could use the exact same code to manipulate regular Julia collections and SQL databases... DBCollections.jl is the package to achieve that!

There is no shortage of Julia packages that provide convenient interface to querying databases – see comparison below. DBCollections.jl is unique in that it allows you reuse the same code, the same functions without any modification for both in-memory collections and databases.

A couple of simple examples:
```julia
# data can either be a regular Julia collection or a `DBCollection`
@p let
    data
    filter(@o _.height > 180 && _.weight < 80)
    map(@o (name=_.name, ratio=_.weight / _.height))
    collect  # optional for Julia collections, required for DBCollections – returns a StructArray for them
end

@p let
    data
    map(@o (i=_.i, b=round(_.i/4.5)))
    group_vg(@o (a=_.i / 3.5,))
    map(@o (a=key(_).a, avg=mean(_.b), cnt=length(_)))
    collect
end
```
More examples and coming, see tests for now.

DBCollections.jl uses FunSQL.jl under the hood, and works with any database supported by it (SQLite, DuckDB, Postgres, MySQL, ...). A large set of operations is already supported by DBCollections:
- `map`/`filter`/`sort`/`Iterators.drop`/`first`/...
- `group` (from FlexiGroups.jl)
- `push!`/`append!`/`copy!`
- `join` (from FlexiJoins.jl) coming soon

From the implementation point of view, defining characteristics of DBCollections.jl are:
- Ease of maintenance: it's a very thin layer converting Julia functions to SQL code. See code sizes in the comparison below.
- No magic, no macros defined, no special parsing/interpolation rules to remember. \
The API consists of regular functions. This also makes DBCollections.jl play nicely with convenience macros from other packages (eg, Accessors and DataPipes).

### Comparison

DBCollections.jl is the only package allowing to reuse the code written for regular Julia collections to operate on database tables as well. \
Still, there are many other packages with roughly similar goals of providing convenient access to SQL databases from Julia. Here, we briefly compare them in terms of main differences and the code size (LOC excluding tests); some Python packages are also included.
  - **DBCollections.jl**: ~250 LOC *(although still growing)*
  - SQLStore.jl: syntax similar to regular Julia, but not 100% and only supports tables created by itself; ~500 LOC
  - dplython: ~700
  - Relationals.jl: ~1200
  - Octo.jl: neat use of Julia comprehensions; ~1800 LOC
  - SQLCompose.jl: closest to regular Julia syntax, still not 100%; ~2000 LOC
  - datar: ~2300
  - PostgresORM.jl: ~2500
  - TidierDB.jl: for those coming from R; ~4000 LOC
  - Blaze: ~10000
  - Ibis: ~43000


### Limitations

- It's impossible to translate 100% of Julia code to SQL. DBCollections aims to support all Julia syntax that can reasonably be translated, so please report if some functionality doesn't work yet!
- For some scalar functions, the SQL semantics may slightly differ between different databases: e.g., `5/3 == 1.6666...` in Julia and many SQL implementations, but in others it is `5/3 == 1`.  DBCollections.jl doesn't perform any unification on top of what FunSQL.jl does.
