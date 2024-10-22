# see also FlexiGroupsExt.jl

struct DBCollectionGrouped
    conn::DBInterface.Connection
    query::FunSQL.AbstractSQLNode
    keyf
end
