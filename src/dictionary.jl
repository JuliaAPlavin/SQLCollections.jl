struct SQLDictionary{I,T}
    coll
    prepared::NamedTuple
end

function SQLDictionary{I,T}(coll::SQLCollection) where {I,T}
    @assert I <: NamedTuple
    @assert T <: NamedTuple
    if !exists(coll)
        coll = _create!(coll,
            NamedTuple{_colnames(:k_, I), Tuple{_coltypes(I)...}},
            NamedTuple{_colnames(:v_, T), Tuple{_coltypes(T)...}})
    end
    colnames = (_colnames(:k_, I)..., _colnames(:v_, T)...)
    prepared = (
        haskey=DBInterface.prepare(coll.conn, """
            SELECT 1
            FROM $(_tablename(coll))
            WHERE $(@p _colnames(:k_, I) map("$_ = ?") join(__, " AND "))
            LIMIT 2
            """),
        getindex=DBInterface.prepare(coll.conn, """
            SELECT $(@p _colnames(:v_, T) join(__, ", "))
            FROM $(_tablename(coll))
            WHERE $(@p _colnames(:k_, I) map("$_ = ?") join(__, " AND "))
            LIMIT 2
            """),
        insert=DBInterface.prepare(coll.conn, """
            INSERT INTO
            $(_tablename(coll)) ($(join(colnames, ", ")))
            VALUES ($(join(fill("?", length(colnames)), ", ")))
            """),
        delete=DBInterface.prepare(coll.conn, """
            DELETE FROM $(_tablename(coll))
            WHERE $(@p _colnames(:k_, I) map("$_ = ?") join(__, " AND "))
            RETURNING 1
            """),
        setindex=DBInterface.prepare(coll.conn, """
            UPDATE $(_tablename(coll))
            SET $(@p _colnames(:v_, T) map("$_ = ?") join(__, ", "))
            WHERE $(@p _colnames(:k_, I) map("$_ = ?") join(__, " AND "))
            RETURNING 1
            """),
        getexcl=DBInterface.prepare(coll.conn, """
            INSERT INTO
            $(_tablename(coll)) ($(join(colnames, ", ")))
            VALUES ($(join(fill("?", length(colnames)), ", ")))
            ON CONFLICT ($(join(_colnames(:k_, I), ", ")))
            DO UPDATE SET rowid = rowid
            RETURNING $(join(_colnames(:v_, T), ", "))
            """),
        set=DBInterface.prepare(coll.conn, """
            INSERT INTO
            $(_tablename(coll)) ($(join(colnames, ", ")))
            VALUES ($(join(fill("?", length(colnames)), ", ")))
            ON CONFLICT ($(join(_colnames(:k_, I), ", ")))
            DO UPDATE SET
            $(@p _colnames(:v_, T) map("$_ = excluded.$_") join(__, ", "))
            RETURNING 1
            """)
    )
    @invoke SQLDictionary{I,T}(coll::Any, prepared)
end

SQLDictionary{I,T}(conn, tblname::Union{Symbol,AbstractString}) where {I,T} = SQLDictionary{I,T}(SQLCollection(conn, tblname))

Base.length(d::SQLDictionary) = length(d.coll)
Base.isempty(d::SQLDictionary) = isempty(d.coll)

Base.collect(d::SQLDictionary) = collect(map(_valoptic(d), d.coll))
Base.first(d::SQLDictionary) = first(map(_valoptic(d), d.coll))

Base.keys(d::SQLDictionary) = map(_keyoptic(d), d.coll)

Dictionaries.issettable(::SQLDictionary) = true
Dictionaries.isinsertable(::SQLDictionary) = true

function Base.haskey(d::SQLDictionary{I}, i) where {I}
    res = DBInterface.execute(d.prepared.haskey, _to_tup(I, i)) |> Tables.rowtable
    @assert length(res) ≤ 1 "Didn't expect multiple values for key $i, got $(length(vals))"
    return !isempty(res)
end

function Base.getindex(d::SQLDictionary{I}, i) where {I}
    res = DBInterface.execute(d.prepared.getindex, _to_tup(I, i)) |> Tables.rowtable
    @assert length(res) ≤ 1 "Didn't expect multiple values for key $i, got $(length(vals))"
    isempty(res) && throw(KeyError(i))
    return _valoptic(d)(only(res))
end

function Base.setindex!(d::SQLDictionary{I,T}, v::NamedTuple, i) where {I,T}
    res = DBInterface.execute(d.prepared.setindex, _to_tup(T, v, I, i)) |> Tables.rowtable
    @assert length(res) ≤ 1 "Didn't expect multiple values for key $i, got $(length(res)); updated all of them"
    isempty(res) && throw(KeyError(i))
    return d
end

Dictionaries.unset!(d::SQLDictionary, i) = delete!(d, i; _strict=false)

function Base.delete!(d::SQLDictionary{I}, i; _strict=true) where {I}
    res = DBInterface.execute(d.prepared.delete, _to_tup(I, i)) |> Tables.rowtable
    @assert length(res) ≤ 1 "Didn't expect multiple values for key $i, got $(length(res)); deleted all of them"
    _strict && isempty(res) && throw(KeyError(i))
    return d
end

function Base.get!(d::SQLDictionary{I,T}, i, default) where {I,T}
    res = DBInterface.execute(d.prepared.getexcl, _to_tup(I, i, T, default)) |> Tables.rowtable
    @assert length(res) ≤ 1 "Didn't expect multiple values for key $i, got $(length(res))"
    return _valoptic(d)(only(res))
end

function Dictionaries.set!(d::SQLDictionary{I,T}, i, v) where {I,T}
    res = DBInterface.execute(d.prepared.set, _to_tup(I, i, T, v)) |> Tables.rowtable
    @assert length(res) ≤ 1 "Didn't expect multiple values for key $i, got $(length(res)); updated all of them"
    return d
end

function Base.insert!(d::SQLDictionary{I,T}, i, v) where {I,T}
    try
        res = DBInterface.execute(d.prepared.insert, _to_tup(I, i, T, v)) |> Tables.rowtable
    catch e
        occursin(r"unique constraint failed"i, string(e)) ?
            throw(IndexError("already contains key $i")) :
            rethrow()
    end
    return d
end

Base.empty!(d::SQLDictionary) = (empty!(d.coll); d)


_keyoptic(d::SQLDictionary{I,T}) where {I,T} = AccessorsExtra.ContainerOptic(NamedTuple{_colnames(Symbol(""), I)}(PropertyLens.(_colnames(:k_, I))))
_valoptic(d::SQLDictionary{I,T}) where {I,T} = AccessorsExtra.ContainerOptic(NamedTuple{_colnames(Symbol(""), T)}(PropertyLens.(_colnames(:v_, T))))

# _to_tup(::Typex::Tuple) = x
function _to_tup(::Type{<:NamedTuple{KS}}, x::NamedTuple{KSx}) where {KS, KSx}
    issetequal(KS, KSx) || error("Expected keys $KS, got $KSx")
    Tuple(x[KS])
end
# _to_tup(x) = (x,)
_to_tup(Tx, x, Ty, y) = (_to_tup(Tx, x)..., _to_tup(Ty, y)...)


_colnames(prefix::Symbol, ::Type{<:NamedTuple{KS}}) where {KS} = Symbol.(prefix, KS)
_colnames(prefix::Symbol, ::Type) = (prefix,)

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
