Base.filter(pred, dbc::DBCollection) = @modify(dbc.query) do q
	q |> Where(func_to_funsql(pred))
end

Base.map(func, dbc::DBCollection) = error("DBCollections: mapping $func not supported")
Base.map(func::Accessors.IndexLens{<:Tuple{NTuple{<:Any,Symbol}}}, dbc::DBCollection) = @modify(dbc.query) do q
	q |> Select(only(func.indices)...)
end
Base.map(func::AccessorsExtra.ContainerOptic{<:NamedTuple}, dbc::DBCollection) = @modify(dbc.query) do q
	q |> Select(map(keys(func.optics), values(func.optics)) do k, o
		k => func_to_funsql(o)
	end...)
end

Base.sort(dbc::DBCollection; by, rev=false) = @modify(dbc.query) do q
	if rev
		q |> Order(map(Desc(), funcs_to_funsql(by))...)
	else
		q |> Order(funcs_to_funsql(by)...)
	end
end

Base.unique(dbc::DBCollection) = @modify(dbc.query) do q
	q |> Group(colnames(dbc)...) |> Select(colnames(dbc)...)
end

Base.first(dbc::DBCollection, n::Integer) = @modify(dbc.query) do q
	q |> Limit(n)
end

Iterators.drop(dbc::DBCollection, n::Integer) = @modify(dbc.query) do q
	q |> Limit(n, typemax(Int))
end

Base.first(dbc::DBCollection) = first(dbc, 1) |> collect |> first

Base.only(dbc::DBCollection) = first(dbc, 2) |> collect |> only

Base.count(pred, dbc::DBCollection) = @modify(dbc.query) do q
	q |> Where(func_to_funsql(pred)) |> Group() |> Select(Agg.count())
end |> only |> only

for f in [:sum, :maximum, :minimum]
	@eval Base.$f(func, dbc::DBCollection) = @modify(dbc.query) do q
		q |> Group() |> Select(aggfunc_to_funsql($f ∘ func))
	end |> only |> only
end

Base.extrema(func, dbc::DBCollection) = @modify(dbc.query) do q
	q |> Group() |> Select(Agg.min(func_to_funsql(func)), Agg.max(func_to_funsql(func)))
end |> only |> NTuple{2}

