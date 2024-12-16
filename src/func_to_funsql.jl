funcs_to_funsql(f) = (func_to_funsql(f),)

func_to_funsql(f) = func_to_funsql(f, nothing)

func_to_funsql(f::ComposedFunction, arg) = func_to_funsql(f.outer, func_to_funsql(f.inner, arg))
func_to_funsql(f::Returns, arg) = f.value
func_to_funsql(f::PropertyLens{P}, ::Nothing) where {P} = getproperty(Get, P)
func_to_funsql(f::PropertyLens{P}, arg) where {P} = getproperty(arg, P)  # to support Join as input; clash with nested structs?

func_to_funsql(::typeof(ismissing), arg) = Fun.is_null(arg)
func_to_funsql(::typeof(isnothing), arg) = Fun.is_null(arg)
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
func_to_funsql(f::Base.Fix2{typeof(*),<:AbstractString}, arg) = Fun.concat(arg, f.x)
func_to_funsql(f::Base.Fix1{typeof(*),<:AbstractString}, arg) = Fun.concat(f.x, arg)
func_to_funsql(f::AccessorsExtra.FixArgs{typeof(*)}, arg) = Fun.concat(map(a -> a isa AccessorsExtra.Placeholder ? arg : a, f.args)...)


# string search: like, regex
func_to_funsql(f::Base.Fix2{typeof(startswith),<:AbstractString}, arg) = Fun.like(arg, "$(_escape_for_like(f.x))%")
func_to_funsql(f::Base.Fix2{typeof(endswith),<:AbstractString}, arg) = Fun.like(arg, "%$(_escape_for_like(f.x))")
func_to_funsql(f::Base.Fix2{typeof(contains),<:AbstractString}, arg) = Fun.like(arg, "%$(_escape_for_like(f.x))%")
_escape_for_like(s::AbstractString) = replace(s, '%' => "\\%", '_' => "\\_")

# func_to_funsql(f::Base.Fix2{typeof(contains),<:Regex}, arg) = Fun.regexp_like(arg, string(f.x))
func_to_funsql(f::Base.Fix1{typeof(occursin)}, arg) = func_to_funsql(contains(f.x), arg)

# see https://github.com/JuliaLang/julia/pull/29643 for regex escaping
func_to_funsql(f::Base.Fix2{typeof(replace),<:Pair{<:AbstractString,<:AbstractString}}, arg) = Fun.regexp_replace(arg, replace(f.x[1], r"([()[\]{}?*+\-|^\$\\.&~#\s=!<>|:])" => s"\\\1"), f.x[2])
func_to_funsql(f::Base.Fix2{typeof(replace),<:Pair{<:Regex,<:AbstractString}}, arg) = Fun.regexp_replace(arg, f.x[1].pattern, f.x[2])


func_to_funsql(f::typeof(lowercase), arg) = Fun.lower(arg)
func_to_funsql(f::typeof(uppercase), arg) = Fun.upper(arg)
func_to_funsql(f::typeof(strip), arg) = Fun.trim(arg)
func_to_funsql(f::typeof(lstrip), arg) = Fun.ltrim(arg)
func_to_funsql(f::typeof(rstrip), arg) = Fun.rtrim(arg)
func_to_funsql(f::Base.Fix2{typeof(strip)}, arg) = Fun.trim(arg, _strip_chars_to_sql(f.x))
func_to_funsql(f::Base.Fix2{typeof(lstrip)}, arg) = Fun.ltrim(arg, _strip_chars_to_sql(f.x))
func_to_funsql(f::Base.Fix2{typeof(rstrip)}, arg) = Fun.rtrim(arg, _strip_chars_to_sql(f.x))
_strip_chars_to_sql(s::AbstractChar) = string(s)
_strip_chars_to_sql(s::AbstractVector{<:AbstractChar}) = String(s)


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
