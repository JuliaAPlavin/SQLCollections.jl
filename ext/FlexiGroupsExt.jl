module FlexiGroupsExt

using FlexiGroups
using FlexiGroups.FlexiMaps: mapset
using SQLCollections: AccessorsExtra, SQLCollection, SQLCollectionGrouped, Group, Select, aggfunc_to_funsql
import SQLCollections: func_to_funsql

# see also grouped.jl

FlexiGroups.group_vg(keyf::AccessorsExtra.ContainerOptic, dbc::SQLCollection) =
    SQLCollectionGrouped(dbc.conn, mapset(keyf, dbc).query |> Group(keys(keyf.optics)...), keyf)

Base.map(func::typeof(key), dbc::SQLCollectionGrouped) = SQLCollection(dbc.conn, dbc.query |> Select(keys(dbc.keyf.optics)...))
Base.map(func::AccessorsExtra.ContainerOptic{<:NamedTuple}, dbc::SQLCollectionGrouped) = SQLCollection(dbc.conn,
    dbc.query |> Select(map(keys(func.optics), values(func.optics)) do k, o
        haskey(dbc.keyf.optics, k) ?
		    k => func_to_funsql(o) :
            k => aggfunc_to_funsql(o)
	end...)
)

func_to_funsql(::typeof(key), arg) = arg  # XXX

end
