module DataManipulationExt

using DataManipulation
using DBCollections
using DBCollections: colnames

DataManipulation.uniqueonly(dbc::DBCollection) = unique(dbc) |> only
DataManipulation.uniqueonly(f, dbc::DBCollection) = unique(f, dbc) |> only

DBCollections.ix_to_select(ix::DataManipulation.StaticRegex, dbc) =
    filter(collect(colnames(dbc))) do n
        occursin(DataManipulation.unstatic(typeof(ix)), String(n))
    end

function DBCollections.ix_to_select(ix::Pair{<:DataManipulation.StaticRegex, <:DataManipulation.StaticSubstitution}, dbc)
    regex, subs = DataManipulation.unstatic.(typeof.([ix[1], ix[2]]))
    filtermap(collect(colnames(dbc))) do n
        if occursin(regex, String(n))
            Symbol(replace(String(n), regex => subs)) => n
        end
    end
end

end
