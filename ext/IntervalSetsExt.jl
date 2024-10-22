module IntervalSetsExt

using IntervalSets
import SQLCollections: func_to_funsql, ⩓

func_to_funsql(f::Base.Fix2{typeof(in), <:Interval}, arg) = func_to_funsql(⩓(
	Base.Fix2(isleftopen(f.x) ? (>) : (≥), leftendpoint(f.x)),
	Base.Fix2(isrightopen(f.x) ? (<) : (≤), rightendpoint(f.x)),
), arg)

end
