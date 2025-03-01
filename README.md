# SQLCollections.jl

> [!IMPORTANT]
> Imagine using the same code to manipulate both regular Julia collections and SQL databases... The SQLCollections.jl package makes this a reality!

While many Julia packages offer convenient interfaces for querying databases (see the comparison below), SQLCollections.jl is unique. It allows you to reuse the *same* code and functions, without any modification, for both in-memory collections and databases.

SQLCollections.jl avoids macros, relying instead on a fundamentally function-based interface.  There are no special parsing or interpolation rules to memorize, resulting in less implicit behavior.  This design also allows SQLCollections.jl to integrate seamlessly with convenience macros from other packages, most notably [Accessors.jl](https://github.com/JuliaObjects/Accessors.jl) and [DataPipes.jl](https://github.com/JuliaAPlavin/DataPipes.jl).

Here are some simple examples illustrating how the same code can operate on either a regular Julia collection or an `SQLCollection`:

```julia
using SQLCollections, AccessorsExtra, DataPipes

data = SQLCollection(mydb, :tablename)

# Basic SQLCollections interface:
# Use familiar functions like map/filter, but pass inspectable function objects
# (not anonymous functions like x -> x.a > 0).
filter(Base.Fix2(>, 0) ∘ PropertyLens(:a), data)

# While already functional, this can be cumbersome for complex functions.
# Fortunately, the Accessors.@o macro provides a concise alternative:
filter((@o _.a > 0), data)

# For multi-step data manipulation, DataPipes.jl's @p macro is a perfect fit:
@p let
    data
    filter(@o _.height > 180 && _.weight < 80)
    map(@o (name=_.name, ratio=_.weight / _.height))
    collect  # Optional for Julia collections, required for SQLCollections (returns a StructArray)
end

# Grouping and other operations are also supported:
@p let
    data
    map(@o (i=_.i, b=round(_.i/4.5)))
    group_vg(@o (a=_.i / 3.5,))
    map(@o (a=key(_).a, avg=mean(_.b), cnt=length(_)))
    collect
end
```

More examples are available in the tests (and more documentation is coming later).

SQLCollections.jl leverages [FunSQL.jl](https://github.com/MechanicalRabbit/FunSQL.jl/tree/master) and works with any database supported by it (SQLite, DuckDB, Postgres, MySQL, and more).  A wide range of operations are already supported by SQLCollections.jl:f
- **Base:** `map`, `filter`, `sort`, `Iterators.drop`, `first`, and others.
- **[DataManipulation.jl](https://github.com/JuliaAPlavin/DataManipulation.jl):** `group` and others.
- **Modifications:** `push!`, `append!`, `copy!`.

Coming Soon:
- **[FlexiJoins.jl](https://github.com/JuliaAPlavin/FlexiJoins.jl):** `join`.
- Support for nested structures within SQL tables.

SQLCollections.jl acts as a very thin layer, translating Julia functions into SQL code (see code sizes in the comparison below).  This approach promotes maintainability and exemplifies Julia's composability.

Here's the improved version with better English, readability, and added links:

### Synergies with other packages

- **[DataPipes.jl](https://github.com/JuliaAPlavin/DataPipes.jl) <|>**: while not strictly required, it provides convenient piping functionality when there are multiple data processing steps. This works seamlessly with both regular Julia collections and SQLCollections.

- **[FunSQL.jl](https://github.com/MechanicalRabbit/FunSQL.jl)**: beyond working with database tables as-is, SQLCollections.jl can also accept arbitrary FunSQL queries as input.

- **[QuackIO.jl](https://github.com/JuliaAPlavin/QuackIO.jl) 🐣🦆**: integrates smoothly with SQLCollections.jl, for efficient out-of-memory filtering and processing of CSV, Parquet, and other tables using DuckDB as the underlying engine.

### Alternatives

SQLCollections.jl is the only package that enables direct reuse of standard Julia data manipulation functions with databases. However, several other packages aim to simplify SQL database access from Julia. This section provides a brief comparison, focusing on key differences and code size (lines of code excluding tests). Some Python packages are included for context:

- 🟢🟣🔴 **SQLCollections.jl:** < 400 LOC
- 🟢🟣🔴 **SQLStore.jl:** Ad-hoc syntax, very limited function/table support; predecessor of SQLCollections.jl; ~500 LOC
- 🔵🟡 **dplython:** ~700 LOC
- 🟢🟣🔴 **QuerySQLite.jl:** Experimental Query.jl syntax support for SQLite; conceptually closest to SQLCollections.jl; ~800 LOC
- 🟢🟣🔴 **Relationals.jl:** ORM; ~1200 LOC
- 🟢🟣🔴 **Octo.jl:** Clever use of Julia comprehensions; ~1800 LOC
- 🟢🟣🔴 **SQLCompose.jl:** Close to regular Julia syntax, but not 100%; ~2000 LOC
- 🔵🟡 **datar:** ~2300 LOC
- 🟢🟣🔴 **PostgresORM.jl:** ORM; ~2500 LOC
- 🟢🟣🔴 **TidierDB.jl:** For users coming from R; ~5000 LOC
- 🔵🟡 **Blaze:** ~10000 LOC
- 🔵🟡 **Ibis:** ~43000 LOC

### Limitations

- It's fundamentally impossible to translate *all* Julia code to SQL. SQLCollections.jl strives to support all translatable Julia syntax. Please report any missing functionality!
- The semantics of some scalar functions may differ slightly between database systems (e.g., `5/3 == 1.6666...` in Julia and many SQL implementations, but `5/3 == 1` in others). SQLCollections.jl does not perform any unification beyond what FunSQL.jl provides.
