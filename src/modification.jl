function Base.push!(dbc::SQLCollection, row::NamedTuple)
	tblname = _tablename(dbc)
	names = colnames(dbc)
	if !issetequal(keys(row), names)
		throw(ArgumentError("cannot push! a row with columns different from than the table: $(keys(row)) vs $(names)"))
	end
	DBInterface.execute(dbc.conn, "insert into $tblname values ($(join(fill("?", length(names)), ", ")))", (row[collect(names)]...,))
	return dbc
end

function Base.copy!(dbc::SQLCollection, rows)
	_copy_impl!(dbc.conn.raw, rows, _tablename(dbc))
	return dbc
end

function _copy_impl! end

function Base.empty!(dbc::SQLCollection)
	tblname = _tablename(dbc)
	DBInterface.execute(dbc.conn, "delete from $tblname")
	return dbc
end
