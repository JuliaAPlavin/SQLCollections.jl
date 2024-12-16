# SQLCollections.jl

> [!IMPORTANT]
> Imagine if you could use the exact same code to manipulate regular Julia collections and SQL databases... SQLCollections.jl is the package to achieve that!

There is no shortage of Julia packages that provide convenient interface to querying databases – see comparison below. SQLCollections.jl is unique in that it allows you reuse the same code, the same functions without any modification for both in-memory collections and databases.

SQLCollections doesn't define any macros, the interface is fundamentally function-based. No special parsing/interpolation rules to remember, less implicit magic overall! \
This makes SQLCollections.jl play nicely with convenience macros from other packages. Most useful are [Accessors](https://github.com/JuliaObjects/Accessors.jl) and [DataPipes.jl](https://github.com/JuliaAPlavin/DataPipes.jl).

Some simple examples – here, `data` can either be a regular Julia collection or an `SQLCollection`.
```julia
using SQLCollections, AccessorsExtra, DataPipes

data = SQLCollection(mydb, :tablename)

# the actual SQLCollections interface:
# use familiar functions like map/filter, but need to pass inspectable function objects – not anonymous functions like x -> x.a > 0
filter(Base.Fix2(>, 0) ∘ PropertyLens(:a), data)

# of course, writing nontrivial functions this way is not too convenient
# luckily, the Accessors.@o macro provides a nice alternative:
filter((@o _.a > 0), data)

# for multi-step data manipulation pipelines, the @p macro from DataPipes is a natural fit:
@p let
    data
    filter(@o _.height > 180 && _.weight < 80)
    map(@o (name=_.name, ratio=_.weight / _.height))
    collect  # optional for Julia collections, required for SQLCollections – returns a StructArray for them
end

# grouping and other functions are supported
@p let
    data
    map(@o (i=_.i, b=round(_.i/4.5)))
    group_vg(@o (a=_.i / 3.5,))
    map(@o (a=key(_).a, avg=mean(_.b), cnt=length(_)))
    collect
end
```
More examples and coming, see tests for now.

SQLCollections.jl uses [FunSQL.jl](https://github.com/MechanicalRabbit/FunSQL.jl/tree/master) under the hood, and works with any database supported by it (SQLite, DuckDB, Postgres, MySQL, ...). A large set of operations is already supported by SQLCollections:
- Base: `map`/`filter`/`sort`/`Iterators.drop`/`first`/...
- [DataManipulation.jl](https://github.com/JuliaAPlavin/DataManipulation.jl): `group`/...
- Modifications: `push!`/`append!`/`copy!`

Coming soon:
- [FlexiJoins.jl](https://github.com/JuliaAPlavin/FlexiJoins.jl): `join`
- support for nested structures, translated to JSON operations in SQL

SQLCollections is a very thin layer converting Julia functions to SQL code – see code sizes in the comparison below. This makes it easy to maintain, and is a nice demonstration of Julia composability.

### Alternatives

SQLCollections.jl is the only package allowing to reuse the code written for regular Julia collections to operate on database tables as well. \
Still, there are many other packages with roughly similar goals of providing convenient access to SQL databases from Julia. Here, we briefly compare them in terms of main differences and the code size (LOC excluding tests); some Python packages are also included for context.
  - **SQLCollections.jl**: ~250 LOC *(although still growing)*
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

- It's fundamentally impossible to translate 100% of Julia code to SQL. SQLCollections aims to support all Julia syntax that can reasonably be translated, so please report if some functionality doesn't work yet!
- For some scalar functions, the SQL semantics may slightly differ between different databases: e.g., `5/3 == 1.6666...` in Julia and many SQL implementations, but in others it is `5/3 == 1`.  SQLCollections.jl doesn't perform any unification on top of what FunSQL.jl does.
