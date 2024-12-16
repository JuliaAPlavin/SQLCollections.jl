module DataManipulationExt

using DataManipulation
using DBCollections

DataManipulation.uniqueonly(dbc::DBCollection) = unique(dbc) |> only
DataManipulation.uniqueonly(f, dbc::DBCollection) = unique(f, dbc) |> only

end
