function Base.push!(dbc::DBCollection, row::NamedTuple)
	@assert dbc.query[] isa FunSQL.FromNode
	tblname = dbc.query[].source::Symbol
	names = colnames(dbc)
	@assert issetequal(keys(row), names)  (keys(row), names)
	DBInterface.execute(dbc.conn, "insert into $tblname values ($(join(fill("?", length(names)), ", ")))", (row[collect(names)]...,))
	return dbc
end

function Base.copy!(dbc::DBCollection, rows)
	@assert dbc.query[] isa FunSQL.FromNode
	tblname = dbc.query[].source::Symbol
	_copy_impl!(dbc.conn.raw, rows, tblname)
	return dbc
end

function _copy_impl! end
