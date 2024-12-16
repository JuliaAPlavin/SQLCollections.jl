module DBCollections

using Tables
using StructArrays
using AccessorsExtra
using FunSQL
using FunSQL: As, From, Fun, Get, Join, Select, Where, Order, Desc, Limit, Agg, Group
using DBInterface

export DBCollection, exists

struct DBCollection
    conn::FunSQL.SQLConnection
    query::FunSQL.AbstractSQLNode
end

DBCollection(conn, tbl::Symbol) = DBCollection(
    FunSQL.DB(conn; catalog=FunSQL.reflect(conn)),
    From(tbl)
)

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

Base.collect(dbc::DBCollection) = DBInterface.execute(dbc.conn, dbc.query) |> columntable |> StructArray

include("func_to_funsql.jl")
include("readfuncs.jl")
include("modification.jl")
include("grouped.jl")

end
