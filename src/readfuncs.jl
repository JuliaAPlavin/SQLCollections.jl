Iterators.filter(pred, dbc::SQLCollection) = filter(pred, dbc)
Base.filter(pred, dbc::SQLCollection) = @modify(dbc.query) do q
	q |> Where(func_to_funsql(pred))
end

Base.map(func, dbc::SQLCollection) = error("SQLCollections: mapping $func not supported")
Base.map(func::Accessors.IndexLens, dbc::SQLCollection) = @modify(dbc.query) do q
	q |> Select(mapreduce(ix -> collect(ix_to_select(ix, dbc)), vcat, func.indices)...)
end
Base.map(func::AccessorsExtra.ContainerOptic{<:NamedTuple}, dbc::SQLCollection) = @modify(dbc.query) do q
	q |> Select(map(keys(func.optics), values(func.optics)) do k, o
		k => func_to_funsql(o)
	end...)
end

ix_to_select(ix::Union{NTuple{<:Any,Symbol},Vector{Symbol}}, dbc) = ix

Base.sort(dbc::SQLCollection; by, rev=false) = @modify(dbc.query) do q
	if rev
		q |> Order(map(Desc(), funcs_to_funsql(by))...)
	else
		q |> Order(funcs_to_funsql(by)...)
	end
end

Base.unique(dbc::SQLCollection) = @modify(dbc.query) do q
	q |> Group(colnames(dbc)...) |> Select(colnames(dbc)...)
end

Base.first(dbc::SQLCollection, n::Integer) = Iterators.take(dbc, n)

Iterators.take(dbc::SQLCollection, n::Integer) = @modify(dbc.query) do q
	q |> Limit(n)
end

Iterators.drop(dbc::SQLCollection, n::Integer) = @modify(dbc.query) do q
	q |> Limit(n, typemax(Int))
end

Base.first(dbc::SQLCollection) = first(dbc, 1) |> collect |> first

Base.only(dbc::SQLCollection) = first(dbc, 2) |> collect |> only

Base.count(pred, dbc::SQLCollection) = @modify(dbc.query) do q
	q |> Where(func_to_funsql(pred)) |> Group() |> Select(Agg.count())
end |> only |> only

for f in [:sum, :maximum, :minimum]
	@eval Base.$f(func, dbc::SQLCollection) = @modify(dbc.query) do q
		q |> Group() |> Select(aggfunc_to_funsql($f ∘ func))
	end |> only |> only
end

Base.extrema(func, dbc::SQLCollection) = @modify(dbc.query) do q
	q |> Group() |> Select(Agg.min(func_to_funsql(func)), Agg.max(func_to_funsql(func)))
end |> only |> NTuple{2}

