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
        @test_broken exists(tbl)
        tbl = SQLCollection(db, :mytbl)
        @test exists(tbl)
        @test length(tbl) == 10
        empty!(tbl)
        @test exists(tbl)
        @test isempty(tbl)
        SQLCollections.drop!(tbl)
        @test_broken !exists(tbl)
        tbl = SQLCollection(db, :mytbl)
        @test !exists(tbl)
        copy!(tbl, data)
        copy!(tbl, data)
        tbl = SQLCollection(db, :mytbl)
        @test length(tbl) == 10
        
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
            (@f map(@o (a=_.j * 10, b=_.i + 1, c=rad2deg(_.i), d=deg2rad(_.j)))),
            (@f map(@o (a=_.j * 10, b=_.i + _.j + 1))),
            (@f map(@o (a=_.j > 5, b=_.i + _.j + 1, c=rad2deg(_.i) * deg2rad(_.j)))),
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
            (@f sort(by=(@o DataManipulation.rev(_.i)))),
            (@f filter(@o _.j ∈ (0.1, 0.5, 0.6) || _.i > 8) sort(by=(@o _.i))),
            (@f map(@o (a=ifelse(_.i > 6, _.j, 1), b=ifelse(_.i > 6, _.j, -_.j), c=ifelse(true, _.j, -_.j)))),
            (@f sort(by=(@o (_.i, -_.j)), rev=true) first(__, 2)),
            (@f sort(by=(@o (_.i, DataManipulation.rev(_.j)))) first(__, 2)),
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
            function (data)
                d1 = @p data filter(@o _.j > 0.3) map(@o (x=_.i,))
                d2 = @p data filter(@o _.j < 0.8) map(@o (x=_.i,))
                vcat(d1, d2)
            end,
            function (data)
                d1 = @p data filter(@o _.j > 0.3) map(@o (x=_.i,))
                d2 = @p data filter(@o _.j < 0.8) map(@o (x=_.i,))
                union(d1, d2)
            end,
            function (data)
                d1 = @p data filter(@o _.j > 0.3) map(@o (x=_.i,))
                d2 = @p data filter(@o _.j < 0.8) map(@o (x=_.i,))
                intersect(d1, d2)
            end,
            function (data)
                d1 = @p data filter(@o _.j > 0.3) map(@o (x=_.i,))
                d2 = @p data filter(@o _.j < 0.8) map(@o (x=_.i,))
                setdiff(d1, d2)
            end,
        ]
            if f isa Tuple
                dbs, f = f
                any(db_ -> db isa db_, dbs) || continue
            end
            # @info "" f(tbl) f(data)
            cf = collect(f(tbl))
            @test issetequal(cf, f(data))
            @test eltype_compatible(eltype(f(tbl)), eltype(f(data)))
        end

        @testset for f in [
            isempty,
            length,
            (@f count(Returns(true))),
            (@f count(@o _.i > 7)),
            (@f any(Returns(true))),
            (@f any(@o _.i > 7)),
            (@f any(@o _.i > 100)),
            (@f all(Returns(true))),
            (@f all(@o _.i > 7)),
            (@f all(@o _.i > 100)),
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

        # f = @f map(@o complex(_.i, _.j)) filter(@o real(_) > 3) map(abs)
        # @test f(tbl) == f(data)
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
            (@f filter(@o _.i ≥ 2) group_vg(@o (a=round(_.i / 3.5),)) map(key) collect),
            (@f map(@o (i=_.i, b=round(_.i/4.5))) group_vg(@o (a=_.i / 3.5,)) map(@o (a=key(_).a, avg=mean(_.b), cnt=length(_))) collect),
        ]
            @test issetequal(f(tbl), f(data))
            @test f(tbl) == f(data)
        end
    end
end

@testitem "pushing" begin
    using SQLite, DuckDB

    @testset for db in [
        SQLite.DB(),
        DuckDB.DB(),
    ]
        tbl = SQLCollection(db, :mytbl)
        copy!(tbl, [(a=1, b="x")])
        tbl = SQLCollection(db, :mytbl)
        @test collect(tbl) == [(a=1, b="x")]
        DBInterface.transaction(tbl) do
            push!(tbl, (a=2, b="y"))
            push!(tbl, (a=3, b="z"))
        end
        @test collect(tbl) == [(a=1, b="x"), (a=2, b="y"), (a=3, b="z")]
        @test_throws ErrorException DBInterface.transaction(tbl) do
            push!(tbl, (a=4, b="w"))
            error()
            push!(tbl, (a=5, b="v"))
        end
        @test collect(tbl) == [(a=1, b="x"), (a=2, b="y"), (a=3, b="z")]
        # XXX: need STRICT for SQLite
        # @test_throws Exception push!(tbl, (a="z", b="w"))
        # @test collect(tbl) == [(a=1, b="x"), (a=2, b="y")]
    end
end

@testitem "dictionary" begin
    using SQLite, DuckDB

    @testset for db in [
        SQLite.DB(),
        # DuckDB.DB(),
    ]
    # db = SQLite.DB()
        # dct = SQLDictionary{Int,String}(SQLCollection(db, :mytbl))
        # @test isempty(dct)
        # @test length(dct) == 0
        # @test collect(dct.coll) |> isempty
        # @test collect(dct) |> isempty
        # insert!(dct, 1, "a")
        # @test collect(dct)


        dct = SQLDictionary{@NamedTuple{a::Int,b::String}, @NamedTuple{x::Float64,y::String}}(db, :mytbl2)
        @test isempty(dct)
        empty!(dct)
        @test isempty(dct)
        @test length(dct) == 0
        @test collect(dct.coll) |> isempty
        @test collect(dct) |> isempty

        dct_ = SQLDictionary(db, :mytbl2)
        @test fieldnames(keytype(dct)) == fieldnames(keytype(dct))
        @test fieldnames(valtype(dct)) == fieldnames(valtype(dct))

        insert!(dct, (a=1, b="a"), (x=1.1, y="def"))
        DBInterface.transaction(dct) do
            insert!(dct, (a=1, b="b"), (y="xyz", x=1.2))
            insert!(dct, (b="a", a=2), (x=2.1, y="abc"))
        end
        @test_throws "already contains" insert!(dct, (a=2, b="a"), (x=2.1, y="abc"))
        @test_throws "cannot store REAL" insert!(dct, (a=2.123, b="a"), (x="xx", y="abc"))
        @test_throws Exception insert!(dct, (a=2, b="a", c="d"), (x="xx", y="abc"))
        @test_throws Exception insert!(dct, (a=2, b="a"), (x="xx", y="abc", c="d"))

        @test length(dct) == 3
        @test collect(dct) == [(x=1.1, y="def"), (x=1.2, y="xyz"), (x=2.1, y="abc")]
        @test first(dct) == (x=1.1, y="def")

        @test dct[(a=1, b="a")] == (x=1.1, y="def")
        @test dct[(b="a", a=1)] == (x=1.1, y="def")
        @test_throws KeyError((a=1, b="c")) dct[(a=1, b="c")]
        @test_throws KeyError((a=1, b="c")) dct[(a=1, b="c")] = (x=1.3, y="ghi")
        @test_throws Exception dct[123]
        @test_throws Exception dct[(a=1, b="c", c="d")]
        dct[(a=1, b="a")] = (x=1.3, y="ghi")
        @test dct[(a=1, b="a")] == (x=1.3, y="ghi")

        @test haskey(dct, (a=1, b="a"))
        @test !haskey(dct, (a=1, b="c"))
        @test_throws Exception haskey(dct, (a=1, b="c", c="d"))
        @test_throws Exception haskey(dct, 123)

        delete!(dct, (a=1, b="a"))
        @test_throws KeyError((a=1, b="a")) delete!(dct, (a=1, b="a"))
        @test collect(dct) == [(x=1.2, y="xyz"), (x=2.1, y="abc")]

        set!(dct, (a=10, b="a"), (x=10.1, y="def"))
        set!(dct, (a=1, b="b"), (x=20, y="XXX"))
        @test collect(dct) == [(x = 20.0, y = "XXX"), (x = 2.1, y = "abc"), (x = 10.1, y = "def")]

        unset!(dct, (a=1, b="b"))
        unset!(dct, (a=100, b="a"))
        @test collect(dct) == [(x = 2.1, y = "abc"), (x = 10.1, y = "def")]

        @test get!(dct, (a=2, b="a"), (x=0.0, y="")) == (x=2.1, y="abc")
        @test get!(dct, (a=2, b="b"), (x=0.0, y="")) == (x=0.0, y="")
        @test collect(dct) == [(x = 2.1, y = "abc"), (x = 10.1, y = "def"), (x = 0.0, y = "")]

        @test collect(keys(dct)) == [(a = 2, b = "a"), (a = 2, b = "b"), (a = 10, b = "a")]

        dct = SQLDictionary{@NamedTuple{a::Int,b::Vector}, @NamedTuple{x::Float64}}(db, :mytbl3)
        insert!(dct, (a=1, b=["a"]), (;x=1.1))
        insert!(dct, (a=1, b=UInt8[1, 2, 3]), (;x=1.2))
        @test issetequal(collect(dct), [(x = 1.1,), (x = 1.2,)])
        @test issetequal(collect(keys(dct)), [(a = 1, b = ["a"]), (a = 1, b = UInt8[1, 2, 3])])
        @test haskey(dct, (a=1, b=["a"]))
        @test haskey(dct, (a=1, b=UInt8[1, 2, 3]))
        @test !haskey(dct, (a=1, b=UInt8[1, 2, 3, 4]))
        @test dct[(b=["a"], a=1)] == (;x=1.1)

        # dct = SQLDictionary{@NamedTuple{a::Int,b::Any}, @NamedTuple{x::Float64}}(db, :mytbl3)
        # @test_broken insert!(dct, (a=1, b="a"), (;x=1.1))
        # insert!(dct, (a=1, b=["a"]), (;x=1.1))
        # @test_broken insert!(dct, (a=1, b=nothing), (;x=1.2))
        # @test collect(dct) == [(x = 1.1,)]
        # @test collect(keys(dct)) == [(a = 1, b = ["a"])]

        # dct = SQLDictionary{@NamedTuple{a::Int,b::Union{Nothing,Vector}}, @NamedTuple{x::Float64}}(db, :mytbl4)
        # @test_broken insert!(dct, (a=1, b="a"), (;x=1.1))
        # insert!(dct, (a=1, b=["a"]), (;x=1.1))
        # insert!(dct, (a=1, b=nothing), (;x=1.2))

        empty!(dct)
        @test isempty(dct)
    end
end 

@testitem "_" begin
    import Aqua
    Aqua.test_all(SQLCollections; ambiguities=(;broken=false))

    import CompatHelperLocal as CHL
    CHL.@check(checktest=false)
end
