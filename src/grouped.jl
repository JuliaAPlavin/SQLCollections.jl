# see also FlexiGroupsExt.jl

struct SQLCollectionGrouped
    conn::DBInterface.Connection
    query::FunSQL.AbstractSQLNode
    keyf
end
