# XXX: should be split into a separate package, only housed here temporarily

struct SQLDictionary{I,T}
    coll
    prepared::NamedTuple
end

function SQLDictionary{I,T}(coll::SQLCollection) where {I,T}
    if !exists(coll)
        coll = _create!(coll,
            NamedTuple{_colnames(:k_, I), Tuple{_coltypes(I)...}},
            NamedTuple{_colnames(:v_, T), Tuple{_coltypes(T)...}})
    end
    colnames = (_colnames(:k_, I)..., _colnames(:v_, T)...)
    qi(x) = _quote_ident(x, coll)
    qtbl = qi(_tablename(coll))
    prepared = (
        haskey=DBInterface.prepare(coll.conn, """
            SELECT 1
            FROM $qtbl
            WHERE $(@p _colnames(:k_, I) map("$(qi(_)) = ?") join(__, " AND "))
            LIMIT 2
            """),
        getindex=DBInterface.prepare(coll.conn, """
            SELECT $(@p _colnames(:v_, T) map(qi) join(__, ", "))
            FROM $qtbl
            WHERE $(@p _colnames(:k_, I) map("$(qi(_)) = ?") join(__, " AND "))
            LIMIT 2
            """),
        insert=DBInterface.prepare(coll.conn, """
            INSERT INTO
            $qtbl ($(join(qi.(colnames), ", ")))
            VALUES ($(join(fill("?", length(colnames)), ", ")))
            """),
        delete=DBInterface.prepare(coll.conn, """
            DELETE FROM $qtbl
            WHERE $(@p _colnames(:k_, I) map("$(qi(_)) = ?") join(__, " AND "))
            RETURNING 1
            """),
        setindex=DBInterface.prepare(coll.conn, """
            UPDATE $qtbl
            SET $(@p _colnames(:v_, T) map("$(qi(_)) = ?") join(__, ", "))
            WHERE $(@p _colnames(:k_, I) map("$(qi(_)) = ?") join(__, " AND "))
            RETURNING 1
            """),
        getexcl=DBInterface.prepare(coll.conn, """
            INSERT INTO
            $qtbl ($(join(qi.(colnames), ", ")))
            VALUES ($(join(fill("?", length(colnames)), ", ")))
            ON CONFLICT ($(join(qi.(_colnames(:k_, I)), ", ")))
            DO UPDATE SET rowid = rowid
            RETURNING $(join(qi.(_colnames(:v_, T)), ", "))
            """),
        set=DBInterface.prepare(coll.conn, """
            INSERT INTO
            $qtbl ($(join(qi.(colnames), ", ")))
            VALUES ($(join(fill("?", length(colnames)), ", ")))
            ON CONFLICT ($(join(qi.(_colnames(:k_, I)), ", ")))
            DO UPDATE SET
            $(@p _colnames(:v_, T) map("$(qi(_)) = excluded.$(qi(_))") join(__, ", "))
            RETURNING 1
            """)
    )
    @invoke SQLDictionary{I,T}(coll::Any, prepared)
end

function SQLDictionary(coll::SQLCollection)
    @assert exists(coll)
    nts = @p let
        eltype(coll)
        collect(zip(fieldnames(__), fieldtypes(__)))
        map() do (k, T)
            ks = string(k)
            if startswith(ks, "k_")
                (kind=:key, name=Symbol(chopprefix(ks, "k_")), type=T)
            elseif startswith(ks, "v_")
                (kind=:value, name=Symbol(chopprefix(ks, "v_")), type=T)
            else
                error("Expected column name to start with 'k_' or 'v_', got $k")
            end
        end
        Tuple
    end
    I = @p nts filter(_.kind == :key) map(_.name => _.type) NamedTuple{first.(__), Tuple{last.(__)...}}
    T = @p nts filter(_.kind == :value) map(_.name => _.type) NamedTuple{first.(__), Tuple{last.(__)...}}
    return SQLDictionary{I,T}(coll)
end

SQLDictionary(conn, tblname::Union{Symbol,AbstractString}) = SQLDictionary(SQLCollection(conn, tblname))
SQLDictionary{I,T}(conn, tblname::Union{Symbol,AbstractString}) where {I,T} = SQLDictionary{I,T}(SQLCollection(conn, tblname))

Base.keytype(::Type{<:SQLDictionary{I}}) where {I} = I
Base.valtype(::Type{<:SQLDictionary{<:Any,T}}) where {T} = T
Base.eltype(::Type{<:SQLDictionary{<:Any,T}}) where {T} = T
for f in (:keytype, :valtype, :eltype)
    @eval Base.$f(d::SQLDictionary) = $f(typeof(d))
end

Base.length(d::SQLDictionary) = length(d.coll)
Base.isempty(d::SQLDictionary) = isempty(d.coll)

Base.collect(d::SQLDictionary{I,T}) where {I,T} = _extract_values(T, collect(map(_valoptic(d), d.coll)))
Base.first(d::SQLDictionary{I,T}) where {I,T} = _extract_value(T, first(map(_valoptic(d), d.coll)))

Base.keys(d::SQLDictionary{<:NamedTuple}) = map(_keyoptic(d), d.coll)
Base.keys(d::SQLDictionary{I}) where {I} = _extract_values(I, collect(map(_keyoptic(d), d.coll)))

Dictionaries.issettable(::SQLDictionary) = true
Dictionaries.isinsertable(::SQLDictionary) = true

function Base.haskey(d::SQLDictionary{I}, i) where {I}
    res = DBInterface.execute(d.prepared.haskey, _to_tup(I, i)) |> Tables.rowtable
    @assert length(res) ≤ 1 "Didn't expect multiple values for key $i, got $(length(res))"
    return !isempty(res)
end

function Base.getindex(d::SQLDictionary{I,T}, i) where {I,T}
    res = DBInterface.execute(d.prepared.getindex, _to_tup(I, i)) |> Tables.rowtable
    @assert length(res) ≤ 1 "Didn't expect multiple values for key $i, got $(length(res))"
    isempty(res) && throw(KeyError(i))
    return _extract_value(T, _valoptic(d)(only(res)))
end

Base.get(d::SQLDictionary, i, default) = get(Returns(default), d, i)

function Base.get(f::Base.Callable, d::SQLDictionary{I,T}, i) where {I,T}
    res = DBInterface.execute(d.prepared.getindex, _to_tup(I, i)) |> Tables.rowtable
    @assert length(res) ≤ 1 "Didn't expect multiple values for key $i, got $(length(res))"
    return isempty(res) ? f() : _extract_value(T, _valoptic(d)(only(res)))
end

function Base.setindex!(d::SQLDictionary{I,T}, v, i) where {I,T}
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
    return _extract_value(T, _valoptic(d)(only(res)))
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


_keyoptic(d::SQLDictionary{I,T}) where {I<:NamedTuple,T} =
    AccessorsExtra.ContainerOptic(NamedTuple{_colnames(Symbol(""), I)}(PropertyLens.(_colnames(:k_, I))))
_valoptic(d::SQLDictionary{I,T}) where {I,T<:NamedTuple} =
    AccessorsExtra.ContainerOptic(NamedTuple{_colnames(Symbol(""), T)}(PropertyLens.(_colnames(:v_, T))))

_keyoptic(d::SQLDictionary{I,T}) where {I,T} =
    AccessorsExtra.ContainerOptic(NamedTuple{(:_,)}((PropertyLens(:k_),)))
_valoptic(d::SQLDictionary{I,T}) where {I,T} =
    AccessorsExtra.ContainerOptic(NamedTuple{(:_,)}((PropertyLens(:v_),)))


function _to_tup(::Type{<:NamedTuple{KS}}, x::NamedTuple{KSx}) where {KS, KSx}
    issetequal(KS, KSx) || error("Expected keys $KS, got $KSx")
    Tuple(x[KS])
end
function _to_tup(::Type{T}, x) where {T}
    @assert !(T <: NamedTuple || T <: Tuple)
    (x,)
end
_to_tup(Tx, x, Ty, y) = (_to_tup(Tx, x)..., _to_tup(Ty, y)...)

_extract_values(::Type{<:NamedTuple}, x) = x
_extract_values(::Type{T}, x) where {T} = map(nt -> nt._, x)
_extract_value(::Type{<:NamedTuple}, x::NamedTuple) = x
_extract_value(::Type{T}, x::NamedTuple{(:_,)}) where {T} = x._::T


_colnames(prefix::Symbol, ::Type{<:NamedTuple{KS}}) where {KS} = Symbol.(prefix, KS)
_colnames(prefix::Symbol, ::Type) = (prefix,)

_coltypes(T::Type{<:NamedTuple}) = fieldtypes(T)
_coltypes(T::Type) = (T,)

function _create!(dbc::SQLCollection, Ts::Type...)
	tblname = _tablename(dbc)
    _create_impl!(dbc.conn.raw, tblname, dbc.conn.catalog.dialect, Ts...)
	return SQLCollection(dbc.conn.raw, tblname)
end

function _tablename(dbc::SQLCollection)
    dbc.query[] isa FunSQL.FromNode || error("Cannot determine tablename of an SQLCollection that is not a plain table")
    return dbc.query[].source::Symbol
end

_tablename(d::SQLDictionary) = _tablename(d.coll)

function _create_impl! end
