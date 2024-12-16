using TestItems
using TestItemRunner
@run_package_tests

@testitem "basic usage" begin
    using SQLite, SQLCipher, DuckDB
    using DataManipulation
    using IntervalSets
    using Statistics

    data = [(;i, j=i/10) for i in 1:10]

    @testset for db in [
        SQLite.DB(),
        # SQLCipher.DB(),
        DuckDB.DB(),
    ]
        tbl = DBCollection(db, :mytbl)
        @test !exists(tbl)
        @test copy!(tbl, data) === tbl
        tbl = DBCollection(db, :mytbl)
        @test exists(tbl)
        @testset for f in [
            # length,
            collect,
            (@f count(Returns(true))),
            (@f count(@o _.i > 7)),
            (@f filter(@o _.i > 7) collect),
            (@f filter(@o _.i ≥ 7) collect),
            (@f filter(@o _.i < 2) collect),
            (@f filter(@o _.i ≤ 2) collect),
            (@f filter(@o _.i == 2) collect),
            (@f filter(@o _.i != 2) collect),
            (@f filter(@o 1 ≤ _.i < 5) collect),
            (@f filter(@o _.j ∈ 0.25..0.75) collect),
            (@f filter(@o _.j ∉ 0.25..0.75) collect),
            (@f filter(@o _.j ∈ (0.1, 0.5, 0.6)) collect),
            (@f filter(@o _.j ∉ [0.1, 0.5, 0.6]) collect),
            (@f map(@o (a=_.j + 1,)) collect),
            (@f map(@o (a=_.j * 10, b=_.i + 1)) collect),
            (@f map(@o (a=ifelse(_.i > 6, 1, 0),)) collect),
            (@f mapinsert(a=@o Float64(_.i) / 2) collect),
            (@f mapset(i=@o round(2*_.j)) collect),
            (@f filter(@o _.i != 2) map(@o (a=ifelse(_.i > 6, 1, 0),)) filter(@o _.a == 1) collect),
            (@f map(@o (a=ifelse(_.i > 6, 1, 0),)) unique() collect),
            (@f sort(by=(@o _.i)) collect),
            (@f sort(by=(@o _.i), rev=true) collect),
            (@f filter(@o _.j ∈ (0.1, 0.5, 0.6) || _.i > 8) sort(by=(@o _.i)) collect),
            (@f sort(by=(@o (_.i, -_.j)), rev=true) first(__, 2) collect),
            (@f sort(by=(@o (_.i, -_.j)), rev=true) Iterators.drop(__, 5) first(__, 2) collect),
            (@f mean(@o _.i)),
            (@f filter(@o _.i > 3) sum(@o _.j)),
            (@f filter(@o _.i > 3) extrema(@o _.j)),
            (@f filter(@o _.i > 3) minimum(@o _.j)),
            (@f filter(@o _.i > 3) maximum(@o _.j)),
        ]
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

    data = [(;i, j=i/10) for i in 1:10] |> DBCollections.StructArray

    @testset for db in [
        SQLite.DB(),
        DuckDB.DB(),
    ]
        copy!(DBCollection(db, :mytbl), data)
        tbl = DBCollection(db, :mytbl)
        @testset for f in [
            (@f group_vg(@o (a=round(_.i / 3.5),)) map(key) collect),
            (@f map(@o (i=_.i, b=round(_.i/4.5))) group_vg(@o (a=_.i / 3.5,)) map(@o (a=key(_).a, avg=mean(_.b))) collect),
        ]
            @test issetequal(f(tbl), f(data))
            @test f(tbl) == f(data)
        end
    end
end


@testitem "_" begin
    import Aqua
    Aqua.test_all(DBCollections; ambiguities=false)
    Aqua.test_ambiguities(DBCollections)

    import CompatHelperLocal as CHL
    CHL.@check()
end
