module PrintfExt

import SQLCollections: func_to_funsql, Fun
using Printf

func_to_funsql(f::Base.Fix1{typeof(Printf.format)}, arg) = Fun.printf((f.x::Printf.Format).str.s, arg)

end
