using TestItems
using TestItemRunner
@run_package_tests

@testitem "basic usage" begin
    using SQLite, SQLCipher, DuckDB
    using DataManipulation
    using DataManipulation: @sr_str
    using IntervalSets
    using Statistics
    using Dates
    using Printf: format, Format
    using StructArrays
    using DictArrays

    function eltype_compatible(sql_T, mem_T)
        sql_T == mem_T && return true
        if isconcretetype(sql_T) && sql_T <: NamedTuple && mem_T <: NamedTuple
            return fieldnames(sql_T) == fieldnames(mem_T)
        end
        @warn "" sql_T mem_T
        return false
    end

    # using Logging; ConsoleLogger(stdout, Logging.Debug) |> global_logger

    data = [(;i, j=i/10, s=string('a'+i-1)^(i-1), d=Date(2000+i, i, 2i), dt=DateTime(2000+i, i, 2i, i, 3i, 4i)) for i in 1:10]

    @testset for db in [
        SQLite.DB(),
        # SQLCipher.DB(),
        DuckDB.DB(),
    ]
        tbl = SQLCollection(db, :mytbl)
        @test !exists(tbl)
        @test !exists(SQLCollection(db, "mytbl"))
        @test copy!(tbl, data) === tbl
        tbl = SQLCollection(db, :mytbl)
        @test exists(tbl)
        @test exists(SQLCollection(db, "mytbl"))

        @testset for f in [
            identity,
            (@f filter(@o _.i > 7)),
            (@f filter(@o _.i ≥ 7)),
            (@f filter(@o _.i < 2)),
            (@f filter(@o _.i ≤ 2)),
            (@f filter(@o _.i == 2)),
            (@f filter(@o _.i != 2)),
            (@f filter(@o 1 ≤ _.i < 5)),
            (@f filter(@o _.j ∈ 0.25..0.75)),
            (@f filter(@o _.j ∉ 0.25..0.75)),
            (@f filter(@o _.j ∈ (0.1, 0.5, 0.6))),
            (@f filter(@o _.j ∉ [0.1, 0.5, 0.6])),
            (@f filter(@o _.j < _.i)),
            (@f filter(@o _.j < _.i && _.j ∈ 0.1..0.6)),
            # (@f filter(@o _.j ∈ (0.1, _.i))),
            # (@f filter(@o _.j ∈ 0.1.._.i)),
            ([DuckDB.DB], @f filter(@o year(_.d) > 2005)),
            ([DuckDB.DB], @f filter(@o _.d > Date(2005))),
            (@f map(@o (a=_.j + 1,))),
            (@f map(@o (a=_.j * 10, b=_.i + 1))),
            (@f map(@o (a=_.j * 10, b=_.i + _.j + 1))),
            (@f map(@o (a=_.j > 5, b=_.i + _.j + 1))),
            (@f map(@o (a=ifelse(_.i > 6, 1, 0), b=ismissing(_.i), c=!ismissing(_.i)))),
            (@f map(@o (a=missing, b=ifelse(_.i > 6, 1, missing), c=ismissing(ifelse(_.i > 6, 1, missing)), d=coalesce(ifelse(_.i > 6, 1, missing), 123)))),
            ([DuckDB.DB], @f map(@o (a=year(_.d), b=year(_.dt), c=month(_.d), d=day(_.dt), e=hour(_.dt), f=minute(_.dt), g=second(_.dt)))),
            (@f map(@o _[(:j, :i)])),
            (@f map(@o _[sr"d.*"])),
            (@f map(@o _[sr"i", sr"d(.*)" => ss"ddd\1"])),
            # (@f map(@o (;_[(:j, :i)]...))),
            (@f mapinsert(a=@o Float64(_.i) / 2)),
            (@f mapset(i=@o round(2*_.j))),
            (@f filter(@o _.i != 2) map(@o (a=ifelse(_.i > 6, 1, 0),)) filter(@o _.a == 1)),
            (@f map(@o (a=ifelse(_.i > 6, 1, 0),)) unique()),
            # (@f unique(@o _.i > 6)),
            (@f map(@o (a=string(_.i, "%"), b=string("x: ", _.j) * " and", c=string("y: ", _.i*10, " kg")))),
            (@f map(@o (a=uppercase(_.s), b=lowercase(_.s * "XX"), c=isempty(_.s)))),
            (@f map(@o (a=startswith(_.s, "cc"), b=endswith(_.s, "dd"), d=occursin("eee", _.s), e=startswith(_.s, "a%")))),
            ([DuckDB.DB], @f map(@o (a=occursin("EeE", _.s), b=replace(_.s, r"e+" => "xxx"), c=replace(_.s, "e+" => "xxx"), d=replace(_.s, "bb" => "xxx")))),
            (@f map(@o (a=format(Format("x %d"), _.i),))),
            (@f map(@o (a=" abc" * _.s * " def  ",)) map(@o (a=strip(_.a), b=lstrip(_.a), c=rstrip(_.a)))),
            (@f map(@o (a=" abc" * _.s * " def  ",)) map(@o (a=strip(_.a, ' '), b=lstrip(_.a, ['a',' ','x']), c=rstrip(_.a, ['f',' ','x'])))),
            (@f sort(by=(@o _.i))),
            (@f sort(by=(@o _.i), rev=true)),
            (@f filter(@o _.j ∈ (0.1, 0.5, 0.6) || _.i > 8) sort(by=(@o _.i))),
            (@f sort(by=(@o (_.i, -_.j)), rev=true) first(__, 2)),
            (@f sort(by=(@o (_.i, -_.j)), rev=true) Iterators.drop(__, 5) Iterators.take(__, 2) first(__, 3)),
        ]
            if f isa Tuple
                dbs, f = f
                any(db_ -> db isa db_, dbs) || continue
            end
            # @info "" f(tbl) f(data)
            cf = collect(f(tbl))
            @test issetequal(cf, f(data))
            @test isequal(cf, f(data))
            @test eltype_compatible(eltype(f(tbl)), eltype(f(data)))

            @testset for g in [
                Array,
                Vector,
                StructArray,
                DictArray,
            ]
                gf = g(f(tbl))
                gd = g(f(data))
                @test nameof(typeof(gf)) == nameof(typeof(gd))
                @test isequal(gf, gd)
            end
        end

        @testset for f in [
            # length,
            (@f count(Returns(true))),
            (@f count(@o _.i > 7)),
            (@f sort(by=(@o (_.i, -_.j)), rev=true) Iterators.drop(__, 5) first),
            (@f sort(by=(@o (_.i, -_.j)), rev=true) Iterators.drop(__, 5) first(__, 1) only),
            (@f mean(@o _.i)),
            (@f filterfirst(@o _.i > 3)),
            (@f filteronly(@o _.i == 3)),
            (@f filter(@o _.i > 3) sum(@o _.j)),
            (@f filter(@o _.i > 3) extrema(@o _.j)),
            (@f filter(@o _.i > 3) minimum(@o _.j)),
            (@f filter(@o _.i > 3) maximum(@o _.j)),
            (@f filter(@o _.i > 3) map(@o (;a=_.i > 3)) uniqueonly()),
        ]
            if f isa Tuple
                dbs, f = f
                any(db_ -> db isa db_, dbs) || continue
            end
            # @info "" f(tbl) f(data)
            @test issetequal(f(tbl), f(data))
            @test f(tbl) == f(data)
        end
    end
end

@testitem "groups" begin
    using SQLite, DuckDB
    using DataManipulation
    using IntervalSets
    using Statistics

    data = [(;i, j=i/10) for i in 1:10] |> SQLCollections.StructArray

    @testset for db in [
        SQLite.DB(),
        DuckDB.DB(),
    ]
        copy!(SQLCollection(db, :mytbl), data)
        tbl = SQLCollection(db, :mytbl)
        @testset for f in [
            (@f group_vg(@o (a=round(_.i / 3.5),)) map(key) collect),
            # (@f group_vg(@o _[(:i,)]) map(key) collect),
            (@f map(@o (i=_.i, b=round(_.i/4.5))) group_vg(@o (a=_.i / 3.5,)) map(@o (a=key(_).a, avg=mean(_.b), cnt=length(_))) collect),
        ]
            @test issetequal(f(tbl), f(data))
            @test f(tbl) == f(data)
        end
    end
end


@testitem "_" begin
    import Aqua
    Aqua.test_all(SQLCollections; ambiguities=false)
    Aqua.test_ambiguities(SQLCollections)

    import CompatHelperLocal as CHL
    CHL.@check(checktest=false)
end
