module DBCollections

using Tables
using StructArrays
using AccessorsExtra
using DataPipes
using FunSQL
using FunSQL: As, From, Fun, Get, Join, Select, Where, Order, Desc, Limit, Agg, Group
using DBInterface

export DBCollection, exists

struct DBCollection
    conn::FunSQL.SQLConnection
    query::FunSQL.AbstractSQLNode
end

DBCollection(conn, tbl::FunSQL.AbstractSQLNode) = DBCollection(
    FunSQL.DB(conn; catalog=FunSQL.reflect(conn)),
    tbl
)

DBCollection(conn, tbl::Union{Symbol,AbstractString}) = DBCollection(conn, From(Symbol(tbl)))

colnames(dbc::DBCollection) = colnames(dbc.query; dbc.conn.catalog)
colnames(q; catalog=nothing) = keys(q.label_map)
colnames(q::FunSQL.SQLNode; kwargs...) = colnames(q[]; kwargs...)
colnames(q::FunSQL.FromNode; catalog) = catalog[q.source::Symbol].columns |> keys

exists(dbc::DBCollection) = 
    try
        colnames(dbc)
        return true
    catch e
        e isa KeyError && return false
        rethrow()
    end

Base.collect(dbc::DBCollection) = StructArray(dbc)

(::Type{StructArray})(dbc::DBCollection) = DBInterface.execute(dbc.conn, dbc.query) |> columntable |> StructArray
(::Type{StructVector})(dbc::DBCollection) = StructArray(dbc)
(::Type{Array})(dbc::DBCollection) = DBInterface.execute(dbc.conn, dbc.query) |> rowtable
(::Type{Vector})(dbc::DBCollection) = Array(dbc)

Tables.istable(::Type{DBCollection}) = true
Tables.columnaccess(::Type{DBCollection}) = true
Tables.columns(dbc::DBCollection) = Tables.columns(DBInterface.execute(dbc.conn, dbc.query))

include("func_to_funsql.jl")
include("readfuncs.jl")
include("modification.jl")
include("grouped.jl")

end
