funcs_to_funsql(f) = (func_to_funsql(f),)

func_to_funsql(f) = func_to_funsql(f, nothing)

func_to_funsql(f::ComposedFunction, arg) = func_to_funsql(f.outer, func_to_funsql(f.inner, arg))
func_to_funsql(f::Returns, arg) = f.value
func_to_funsql(f::PropertyLens{P}, ::Nothing) where {P} = getproperty(Get, P)
func_to_funsql(f::PropertyLens{P}, arg) where {P} = getproperty(arg, P)  # to support Join as input; clash with nested structs?

func_to_funsql(f::Base.Fix2{typeof(in), <:Union{Tuple,AbstractVector}}, arg) = Fun.in(arg, f.x...)
func_to_funsql(f::Base.Fix2{typeof(∉)}, arg) = func_to_funsql(!∈(f.x), arg)

func_to_funsql(f::Base.Fix2, arg) = getproperty(Fun, nameof(f.f))(arg, f.x)
func_to_funsql(f::Base.Fix1, arg) = getproperty(Fun, nameof(f.f))(f.x, arg)
func_to_funsql(f::Function, arg) = getproperty(Fun, nameof(f))(arg)

funcs_to_funsql(f::AccessorsExtra.ContainerOptic) = map(func_to_funsql, f.optics)

func_to_funsql(f::⩓, arg) = Fun.and(func_to_funsql(f.f, arg), func_to_funsql(f.g, arg))
func_to_funsql(f::⩔, arg) = Fun.or(func_to_funsql(f.f, arg), func_to_funsql(f.g, arg))

func_to_funsql(f::AccessorsExtra.FixArgsT(ifelse, (AccessorsExtra.Placeholder, Any, Any)), arg) = Fun.case(arg, f.args[2], f.args[3])

func_to_funsql(::Type{Float64}, arg) = Fun.cast(arg, "REAL")

func_to_funsql(f::Base.Fix2{typeof(string)}, arg) = Fun.concat(arg, f.x)
func_to_funsql(f::Base.Fix1{typeof(string)}, arg) = Fun.concat(f.x, arg)
func_to_funsql(f::AccessorsExtra.FixArgs{typeof(string)}, arg) = Fun.concat(map(a -> a isa AccessorsExtra.Placeholder ? arg : a, f.args)...)


func_to_funsql(f::AccessorsExtra.PropertyFunction, arg) = @p let
	f.expr
	@modify(__ |> RecursiveOfType(Expr; order=:pre) |> If(e -> Base.isexpr(e, :call)) |> _.args[1]) do func
		:($Fun.$func)
	end
	@set __ |> RecursiveOfType(Symbol) |> If(==(:_)) = something(arg, Get)
	eval()
end


aggfunc_to_funsql(f) = aggfunc_to_funsql(f, nothing)
aggfunc_to_funsql(f::ComposedFunction, arg) = aggfunc_to_funsql(f.outer, func_to_funsql(f.inner, arg))
aggfunc_to_funsql(::typeof(minimum), arg) = Agg.min(arg)
aggfunc_to_funsql(::typeof(maximum), arg) = Agg.max(arg)
aggfunc_to_funsql(::typeof(sum), arg) = Agg.sum(arg)
aggfunc_to_funsql(::typeof(length), arg) = Agg.count()
