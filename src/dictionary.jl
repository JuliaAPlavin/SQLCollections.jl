struct SQLDictionary{I,T}
    coll
end

function SQLDictionary{I,T}(coll::SQLCollection) where {I,T}
    @assert I <: NamedTuple
    @assert T <: NamedTuple
    if !exists(coll)
        coll = _create!(coll,
            NamedTuple{_colnames(:k_, I), Tuple{_coltypes(I)...}},
            NamedTuple{_colnames(:v_, T), Tuple{_coltypes(T)...}})
    end
    @invoke SQLDictionary{I,T}(coll::Any)
end

Base.length(d::SQLDictionary) = length(d.coll)  
Base.isempty(d::SQLDictionary) = isempty(d.coll)

Base.collect(d::SQLDictionary) = collect(map(_valoptic(d), d.coll))
Base.first(d::SQLDictionary) = first(map(_valoptic(d), d.coll))

Base.keys(d::SQLDictionary) = map(_keyoptic(d), d.coll)

function Base.getindex(d::SQLDictionary, i)
    vals = @p filter((@o _keyoptic(d)(_) == i), d.coll) map(_valoptic(d)) first(__, 2) collect
    @assert length(vals) ≤ 1 "Didn't expect multiple values for key $i, got $(length(vals))"
    isempty(vals) && throw(KeyError(i))
    return only(vals)
end

function Base.setindex!(d::SQLDictionary, v::NamedTuple, i)
    res = DBInterface.execute(d.coll.conn, """
    UPDATE $(_tablename(d))
    SET $(@p _colnames(:v_, typeof(v)) map("$_ = ?") join(__, ", "))
    WHERE $(@p _colnames(:k_, typeof(i)) map("$_ = ?") join(__, " AND "))
    RETURNING 1
    """, _to_tup(v, i)) |> Tables.rowtable
    @assert length(res) ≤ 1 "Didn't expect multiple values for key $i, got $(length(res)); updated all of them"
    isempty(res) && throw(KeyError(i))
    return d
end

Dictionaries.unset!(d::SQLDictionary, i) = delete!(d, i; _strict=false)

function Base.delete!(d::SQLDictionary, i; _strict=true)
    res = DBInterface.execute(d.coll.conn, """
    DELETE FROM $(_tablename(d))
    WHERE $(@p _colnames(:k_, typeof(i)) map("$_ = ?") join(__, " AND "))
    RETURNING 1
    """, _to_tup(i)) |> Tables.rowtable
    @assert length(res) ≤ 1 "Didn't expect multiple values for key $i, got $(length(res)); deleted all of them"
    _strict && isempty(res) && throw(KeyError(i))
    return d
end

function Dictionaries.set!(d::SQLDictionary, i, v)
    colnames = (_colnames(:k_, typeof(i))..., _colnames(:v_, typeof(v))...)
    res = DBInterface.execute(d.coll.conn, """
    INSERT INTO
    $(_tablename(d)) ($(join(colnames, ", ")))
    VALUES ($(join(fill("?", length(colnames)), ", ")))
    ON CONFLICT ($(join(_colnames(:k_, typeof(i)), ", ")))
    DO UPDATE SET
    $(@p _colnames(:v_, typeof(v)) map("$_ = excluded.$_") join(__, ", "))
    RETURNING 1
    """, _to_tup(i, v)) |> Tables.rowtable
    @assert length(res) ≤ 1 "Didn't expect multiple values for key $i, got $(length(res)); updated all of them"
    return d
end

function Base.insert!(d::SQLDictionary, i, v)
    try
        push!(d.coll, NamedTuple{(_colnames(:k_, typeof(i))..., _colnames(:v_, typeof(v))...)}(_to_tup(i, v)))
    catch e
        occursin(r"unique constraint failed"i, string(e)) ?
            throw(IndexError("already contains key $i")) :
            rethrow()
    end
    return d
end

_keyoptic(d::SQLDictionary{I,T}) where {I,T} = AccessorsExtra.ContainerOptic(NamedTuple{_colnames(Symbol(""), I)}(PropertyLens.(_colnames(:k_, I))))
_valoptic(d::SQLDictionary{I,T}) where {I,T} = AccessorsExtra.ContainerOptic(NamedTuple{_colnames(Symbol(""), T)}(PropertyLens.(_colnames(:v_, T))))

_to_tup(x::Tuple) = x
_to_tup(x::NamedTuple) = Tuple(x)
_to_tup(x) = (x,)
_to_tup(x, y) = (_to_tup(x)..., _to_tup(y)...)


_colnames(prefix::Symbol, ::Type{<:NamedTuple{KS}}) where {KS} = Symbol.(prefix, KS)
_colnames(prefix::Symbol, ::Type) where {KS} = (prefix,)

_coltypes(T::Type{<:NamedTuple}) = fieldtypes(T)
_coltypes(T::Type) = (T,)

function _create!(dbc::SQLCollection, Ts::Type...)
	tblname = _tablename(dbc)
    _create_impl!(dbc.conn.raw, tblname, Ts...)
	return SQLCollection(dbc.conn.raw, tblname)
end

function _tablename(dbc::SQLCollection)
    dbc.query[] isa FunSQL.FromNode || error("Cannot determine tablename of an SQLCollection that is not a plain table")
    return dbc.query[].source::Symbol
end

_tablename(d::SQLDictionary) = _tablename(d.coll)

function _create_impl! end

# * `getindex(::AbstractDictionary{I, T}, ::I) --> T`
# * `isassigned(::AbstractDictionary{I}, ::I) --> Bool`
# * `keys(::AbstractDictionary{I, T}) --> AbstractIndices{I}`

# If values can be set/mutated, then an `AbstractDictionary` should implement:

# * `issettable(::AbstractDictionary)` (returning `true`)
# * `setindex!(dict::AbstractDictionary{I, T}, ::T, ::I}` (returning `dict`)

# If arbitrary indices can be added to or removed from the dictionary, implement:

# * `isinsertable(::AbstractDictionary)` (returning `true`)
# * `insert!(dict::AbstractDictionary{I, T}, ::I, ::T}` (returning `dict`)
# * `delete!(dict::AbstractDictionary{I, T}, ::I}` (returning `dict`)
