module SQLCollections

using Tables
using StructArrays
using AccessorsExtra
using DataPipes
using FunSQL
using FunSQL: As, From, Fun, Get, Join, Select, Where, Order, Desc, Limit, Agg, Group
using DBInterface

export SQLCollection, exists

struct SQLCollection
    conn::FunSQL.SQLConnection
    query::FunSQL.AbstractSQLNode

    function SQLCollection(conn::FunSQL.SQLConnection, query::FunSQL.AbstractSQLNode)
        @debug "creating SQLCollection" query
        new(conn, query)
    end
end

SQLCollection(conn, tbl::FunSQL.AbstractSQLNode) = SQLCollection(
    FunSQL.DB(conn; catalog=FunSQL.reflect(conn)),
    tbl
)

SQLCollection(conn, tbl::Union{Symbol,AbstractString}) = SQLCollection(conn, From(Symbol(tbl)))

colnames(dbc::SQLCollection) = colnames(dbc.query; dbc.conn.catalog)
colnames(q; catalog=nothing) = keys(q.label_map)
colnames(q::FunSQL.SQLNode; kwargs...) = colnames(q[]; kwargs...)
colnames(q::FunSQL.FromNode; catalog) = catalog[q.source::Symbol].columns |> keys

exists(dbc::SQLCollection) = 
    try
        colnames(dbc)
        return true
    catch e
        e isa KeyError && return false
        rethrow()
    end

Base.collect(dbc::SQLCollection) = StructArray(dbc)

(::Type{StructArray})(dbc::SQLCollection) = DBInterface.execute(dbc.conn, dbc.query) |> columntable |> StructArray
(::Type{StructVector})(dbc::SQLCollection) = StructArray(dbc)
(::Type{Array})(dbc::SQLCollection) = DBInterface.execute(dbc.conn, dbc.query) |> rowtable
(::Type{Vector})(dbc::SQLCollection) = Array(dbc)

Tables.istable(::Type{SQLCollection}) = true
Tables.columnaccess(::Type{SQLCollection}) = true
Tables.columns(dbc::SQLCollection) = Tables.columns(DBInterface.execute(dbc.conn, dbc.query))

include("func_to_funsql.jl")
include("readfuncs.jl")
include("modification.jl")
include("grouped.jl")

include("../ext/PrintfExt.jl")

end
