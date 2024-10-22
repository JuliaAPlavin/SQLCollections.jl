module FlexiMapsExt

using FlexiMaps
using DBCollections: DBCollection, func_to_funsql, colnames, Select, @modify, AccessorsExtra


FlexiMaps.mapset(dbc::DBCollection; kwargs...) = @modify(dbc.query) do q
    @assert keys(kwargs) ⊆ colnames(dbc)
    q |> Select(
        map(collect(colnames(dbc))) do k
            haskey(kwargs, k) ?
                k => func_to_funsql(kwargs[k]) :
                k
        end...
    )
end

FlexiMaps.mapset(func::AccessorsExtra.ContainerOptic{<:NamedTuple}, dbc::DBCollection) = @modify(dbc.query) do q
    q |> Select(
        setdiff(colnames(dbc), keys(func.optics))...,
        map(keys(func.optics), values(func.optics)) do k, o
            k => func_to_funsql(o)
        end...
    )
end

FlexiMaps.mapinsert(dbc::DBCollection; kwargs...) = @modify(dbc.query) do q
    q |> Select(colnames(dbc)..., map(keys(kwargs), values(kwargs)) do k, o
        k => func_to_funsql(o)
    end...)
end

end
