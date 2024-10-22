module FlexiGroupsExt

using FlexiGroups
using FlexiGroups.FlexiMaps: mapset
using DBCollections: AccessorsExtra, DBCollection, DBCollectionGrouped, Group, Select, aggfunc_to_funsql
import DBCollections: func_to_funsql

# see also grouped.jl

FlexiGroups.group_vg(keyf::AccessorsExtra.ContainerOptic, dbc::DBCollection) =
    DBCollectionGrouped(dbc.conn, mapset(keyf, dbc).query |> Group(keys(keyf.optics)...), keyf)

Base.map(func::typeof(key), dbc::DBCollectionGrouped) = DBCollection(dbc.conn, dbc.query |> Select(keys(dbc.keyf.optics)...))
Base.map(func::AccessorsExtra.ContainerOptic{<:NamedTuple}, dbc::DBCollectionGrouped) = DBCollection(dbc.conn,
    dbc.query |> Select(map(keys(func.optics), values(func.optics)) do k, o
        haskey(dbc.keyf.optics, k) ?
		    k => func_to_funsql(o) :
            k => aggfunc_to_funsql(o)
	end...)
)

func_to_funsql(::typeof(key), arg) = arg  # XXX

end
