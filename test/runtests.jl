using TestItems
using TestItemRunner
@run_package_tests


@testitem "_" begin
    import Aqua
    Aqua.test_all(DBCollections; ambiguities=false)
    Aqua.test_ambiguities(DBCollections)

    import CompatHelperLocal as CHL
    CHL.@check()
end