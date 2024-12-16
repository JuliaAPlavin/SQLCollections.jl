module StatisticsExt

using Statistics
using SQLCollections: SQLCollection, func_to_funsql, Group, Select, Agg, @modify
import SQLCollections: aggfunc_to_funsql

aggfunc_to_funsql(::typeof(mean), arg) = Agg.avg(arg)

for f in [:mean]
	@eval Statistics.$f(func, dbc::SQLCollection) = @modify(dbc.query) do q
		q |> Group() |> Select(aggfunc_to_funsql($f ∘ func))
	end |> only |> only
end

end
