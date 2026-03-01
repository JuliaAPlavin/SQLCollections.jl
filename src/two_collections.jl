function Base.vcat(dbcs::SQLCollection...)
    @assert all(dbc -> dbc.conn.raw == first(dbcs).conn.raw, dbcs)
    @assert all(dbc -> colnames(dbc) == colnames(first(dbcs)), dbcs)
    dbc_a = first(dbcs)
    qs_rest = map(dbc -> dbc.query, Base.tail(dbcs))
    @modify(dbc_a.query) do q_a
        q_a |> Append(qs_rest...)
    end
end

# can use SQL UNION / INTERSECT / EXCEPT?

function Base.intersect(dbc_a::SQLCollection, dbc_b::SQLCollection)
    @assert dbc_a.conn.raw == dbc_b.conn.raw
    cols = collect(colnames(dbc_a))
    @assert issetequal(cols, colnames(dbc_b))
    jcond = Fun.and(map(col -> Fun.:(==)(Get[col], Get._rhs_[col]), cols)...)
    q_b = dbc_b.query
    @modify(dbc_a.query) do q_a
        q_a |> Join(:_rhs_ => dbc_b.query, jcond) |> Group(cols...) |> Select(cols...)
    end
end

function Base.union(dbc_a::SQLCollection, dbc_b::SQLCollection)
    @assert dbc_a.conn.raw == dbc_b.conn.raw
    cols = collect(colnames(dbc_a))
    @assert issetequal(cols, colnames(dbc_b))
    jcond = Fun.or(map(col -> Fun.:(==)(Get[col], Get._rhs_[col]), cols)...)
    sels = map(col -> col => Fun.coalesce(Get[col], Get._rhs_[col]), cols)
    q_b = dbc_b.query
    @modify(dbc_a.query) do q_a
        q_a |> Join(:_rhs_ => dbc_b.query, jcond, left=true, right=true) |> Select(sels...) |> Group(cols...) |> Select(cols...)
    end
end

function Base.setdiff(dbc_a::SQLCollection, dbc_b::SQLCollection)
    @assert dbc_a.conn.raw == dbc_b.conn.raw
    cols = collect(colnames(dbc_a))
    @assert issetequal(cols, colnames(dbc_b))
    jcond = Fun.and(map(col -> Fun.:(==)(Get[col], Get._rhs_[col]), cols)...)
    wcond = Fun.and(map(col -> Fun.is_null(Get._rhs_[col]), cols)...)
    q_b = dbc_b.query
    @modify(dbc_a.query) do q_a
        q_a |> Join(:_rhs_ => dbc_b.query, jcond, left=true) |> Where(wcond) |> Group(cols...) |> Select(cols...)
    end
end
