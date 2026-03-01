struct MappedLaterSQLCollection{F}
    func::F
    coll::SQLCollection
end

map_later(func, coll::SQLCollection) = MappedLaterSQLCollection(func, coll)

Base.collect(mc::MappedLaterSQLCollection) = map(mc.func, collect(mc.coll))

Base.filter(pred, mc::MappedLaterSQLCollection) = MappedLaterSQLCollection(mc.func, filter(pred, mc.coll))
Iterators.filter(pred, mc::MappedLaterSQLCollection) = filter(pred, mc)

Base.sort(mc::MappedLaterSQLCollection; kwargs...) = MappedLaterSQLCollection(mc.func, sort(mc.coll; kwargs...))

Base.first(mc::MappedLaterSQLCollection, n::Integer) = MappedLaterSQLCollection(mc.func, first(mc.coll, n))
Iterators.take(mc::MappedLaterSQLCollection, n::Integer) = MappedLaterSQLCollection(mc.func, Iterators.take(mc.coll, n))
Iterators.drop(mc::MappedLaterSQLCollection, n::Integer) = MappedLaterSQLCollection(mc.func, Iterators.drop(mc.coll, n))
