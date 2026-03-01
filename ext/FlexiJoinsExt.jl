module FlexiJoinsExt

using SQLCollections
import SQLCollections: SQLCollection, func_to_funsql, colnames, map_later, _to_sql
using FlexiJoins
import FlexiJoins: _flexijoin, JoinCondition, ByKey, ByPred, CompositeCondition, Keep, Drop
using FunSQL: Get, Join, Select, Fun
using AccessorsExtra: @modify, ContainerOptic, PropertyLens, PropertyFunction, Placeholder


# --- Condition → FunSQL ON clause ---

function cond_to_funsql(cond::ByKey)
    if length(cond.keyfuncs) == 1
        f = only(cond.keyfuncs)
        Fun.:(==)(func_to_funsql(f, Get), func_to_funsql(f, Get._rhs_))
    else
        f_L, f_R = cond.keyfuncs
        Fun.:(==)(func_to_funsql(f_L, Get), func_to_funsql(f_R, Get._rhs_))
    end
end

function cond_to_funsql(cond::ByPred)
    getproperty(Fun, Symbol(cond.pred))(
        func_to_funsql(cond.Lf, Get),
        func_to_funsql(cond.Rf, Get._rhs_)
    )
end

cond_to_funsql(cond::CompositeCondition) = Fun.and(map(cond_to_funsql, cond.conds)...)


_find_sql_conn(datas) = (d = first(Iterators.filter(v -> v isa SQLCollection, values(datas))); d.conn)


# --- Build nesting optic from side_cols ---

function _side_optic(side::Symbol, cols::Tuple, ::Drop)
    ContainerOptic(NamedTuple{cols}(map(col -> PropertyLens{Symbol(side, :_, col)}(), cols)))
end

function _side_optic(side::Symbol, cols::Tuple, ::Keep)
    flat_cols = map(col -> Symbol(side, :_, col), cols)
    props = NamedTuple{flat_cols}(ntuple(_ -> Placeholder(), length(cols)))
    vc, vfc = Val(cols), Val(flat_cols)
    func = row -> _keep_side_func(row, vc, vfc)
    PropertyFunction(props, func, nothing)
end

_keep_side_func(row, ::Val{cols}, ::Val{flat_cols}) where {cols, flat_cols} = let
    vals = row[flat_cols]
    all(ismissing, values(vals)) ? nothing : NamedTuple{cols}(values(vals))
end

function _make_nest_optic(side_cols::NamedTuple{NS}, nonmatches) where {NS}
    # Keep on side X means non-matching X rows are kept → the OTHER side gets missings
    other_nm = reverse(nonmatches)
    inner_optics = map(NS, other_nm) do side, nm
        _side_optic(side, side_cols[side], nm)
    end
    ContainerOptic(NamedTuple{NS}(inner_optics))
end


# --- Build the join query ---

function _build_join_query(datas_sql::NamedTuple{NS}, cond, nonmatches) where {NS}
    dbc_L = datas_sql[1]
    dbc_R = datas_sql[2]

    # Build ON clause
    on_clause = cond_to_funsql(cond)

    # Build SELECT with prefixed column names
    cols_L = colnames(dbc_L)
    cols_R = colnames(dbc_R)
    prefix_L = NS[1]
    prefix_R = NS[2]

    select_args = Pair{Symbol,Any}[]
    for col in cols_L
        push!(select_args, Symbol(prefix_L, :_, col) => Get[col])
    end
    for col in cols_R
        push!(select_args, Symbol(prefix_R, :_, col) => Get._rhs_[col])
    end

    join_kw = Dict{Symbol,Any}()
    if nonmatches[1] isa Keep
        join_kw[:left] = true
    end
    if nonmatches[2] isa Keep
        join_kw[:right] = true
    end

    query = @modify(dbc_L.query) do q_L
        q_L |> Join(:_rhs_ => dbc_R.query, on_clause; join_kw...) |> Select(select_args...)
    end

    side_cols = NamedTuple{NS}((Tuple(cols_L), Tuple(cols_R)))
    return query, side_cols
end


# --- Main dispatch ---

function _sql_flexijoin(datas::NamedTuple{NS}, cond::JoinCondition; nonmatches=nothing, kwargs...) where {NS}
    length(datas) == 2 || error("SQL joins only support exactly 2 datasets")

    # Check for unsupported kwargs
    for (k, v) in pairs(kwargs)
        k in (:multi, :groupby) && v !== nothing && error("SQL joins do not support $k")
    end

    conn = _find_sql_conn(datas)

    nm = FlexiJoins.normalize_arg(nonmatches, datas; default=FlexiJoins.drop)
    cond_norm = FlexiJoins.normalize_arg(cond, datas)

    # Convert all sides to SQLCollections
    sql_datas = NamedTuple{NS}(map(v -> _to_sql(conn, v), values(datas)))

    query, side_cols = _build_join_query(sql_datas, cond_norm, nm)
    return map_later(_make_nest_optic(side_cols, nm), query)
end

# Dispatch for at least one SQLCollection side
_flexijoin(datas::NamedTuple{<:Any, <:Tuple{SQLCollection, Any}}, cond::JoinCondition; kwargs...) = _sql_flexijoin(datas, cond; kwargs...)
_flexijoin(datas::NamedTuple{<:Any, <:Tuple{Any, SQLCollection}}, cond::JoinCondition; kwargs...) = _sql_flexijoin(datas, cond; kwargs...)
_flexijoin(datas::NamedTuple{<:Any, <:Tuple{SQLCollection, SQLCollection}}, cond::JoinCondition; kwargs...) = _sql_flexijoin(datas, cond; kwargs...)

end
