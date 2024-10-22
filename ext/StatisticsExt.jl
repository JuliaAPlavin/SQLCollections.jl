module StatisticsExt

using Statistics
using DBCollections: DBCollection, func_to_funsql, Group, Select, Agg, @modify
import DBCollections: aggfunc_to_funsql

aggfunc_to_funsql(::typeof(mean), arg) = Agg.avg(arg)

for f in [:mean]
	@eval Statistics.$f(func, dbc::DBCollection) = @modify(dbc.query) do q
		q |> Group() |> Select(aggfunc_to_funsql($f ∘ func))
	end |> only |> only
end

end
