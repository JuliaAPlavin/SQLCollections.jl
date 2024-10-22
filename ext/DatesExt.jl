module DatesExt

using Dates
import SQLCollections: Fun, func_to_funsql

func_to_funsql(::typeof(year), arg) = Fun.extract("YEAR", arg)
func_to_funsql(::typeof(month), arg) = Fun.extract("MONTH", arg)
func_to_funsql(::typeof(day), arg) = Fun.extract("DAY", arg)
func_to_funsql(::typeof(hour), arg) = Fun.extract("HOUR", arg)
func_to_funsql(::typeof(minute), arg) = Fun.extract("MINUTE", arg)
func_to_funsql(::typeof(second), arg) = Fun.extract("SECOND", arg)

end
