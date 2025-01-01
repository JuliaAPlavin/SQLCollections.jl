module DataManipulationExt

using DataManipulation
using SQLCollections
using SQLCollections: colnames

DataManipulation.uniqueonly(dbc::SQLCollection) = unique(dbc) |> only
DataManipulation.uniqueonly(f, dbc::SQLCollection) = unique(f, dbc) |> only

SQLCollections.ix_to_select(ix::DataManipulation.StaticRegex, dbc) =
    filter(collect(colnames(dbc))) do n
        occursin(DataManipulation.unstatic(typeof(ix)), String(n))
    end

function SQLCollections.ix_to_select(ix::Pair{<:DataManipulation.StaticRegex, <:DataManipulation.StaticSubstitution}, dbc)
    regex, subs = DataManipulation.unstatic.(typeof.([ix[1], ix[2]]))
    filtermap(collect(colnames(dbc))) do n
        if occursin(regex, String(n))
            Symbol(replace(String(n), regex => subs)) => n
        end
    end
end

end
