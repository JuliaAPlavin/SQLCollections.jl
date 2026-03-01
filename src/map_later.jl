struct MappedLaterSQLCollection{F}
    func::F
    coll::SQLCollection
end

"""    map_later(func, coll::SQLCollection)

Wrap `coll` so that `func` is applied in Julia after SQL execution, rather than being translated to SQL.
Downstream operations like `filter`, `sort`, `first` are pushed to SQL as usual.
For inspectable functions (`ContainerOptic`), unconsumed input columns are preserved in the result.

Experimental: interface may change.
"""
map_later(func, coll::SQLCollection) = MappedLaterSQLCollection(func, coll)

Base.collect(mc::MappedLaterSQLCollection) = map(mc.func, collect(mc.coll))

function Base.collect(mc::MappedLaterSQLCollection{<:AccessorsExtra.ContainerOptic{<:NamedTuple}})
    sa = StructArray(mc.coll)
    consumed = keys(AccessorsExtra.propspec(mc.func))
    comps = StructArrays.components(sa)
    kept_keys = Tuple(setdiff(keys(comps), consumed))
    kept = NamedTuple{kept_keys}(comps)
    new_cols = map(_apply_optic_col(sa), mc.func.optics)
    StructArray(merge(kept, new_cols))
end

_apply_optic_col(sa) = optic -> _apply_optic_col(optic, sa)
_apply_optic_col(::PropertyLens{P}, sa) where {P} = getproperty(sa, P)
_apply_optic_col(optic, sa) = map(optic, sa)

Base.filter(pred, mc::MappedLaterSQLCollection) = MappedLaterSQLCollection(mc.func, filter(pred, mc.coll))
Iterators.filter(pred, mc::MappedLaterSQLCollection) = filter(pred, mc)

Base.sort(mc::MappedLaterSQLCollection; kwargs...) = MappedLaterSQLCollection(mc.func, sort(mc.coll; kwargs...))

Base.first(mc::MappedLaterSQLCollection, n::Integer) = MappedLaterSQLCollection(mc.func, first(mc.coll, n))
Iterators.take(mc::MappedLaterSQLCollection, n::Integer) = MappedLaterSQLCollection(mc.func, Iterators.take(mc.coll, n))
Iterators.drop(mc::MappedLaterSQLCollection, n::Integer) = MappedLaterSQLCollection(mc.func, Iterators.drop(mc.coll, n))
