function Base.push!(dbc::SQLCollection, row::NamedTuple)
	tblname = _tablename(dbc)
	qtblname = _quote_ident(tblname, dbc.conn)
	names = colnames(dbc)
	if !issetequal(keys(row), names)
		throw(ArgumentError("cannot push! a row with columns different from than the table: $(keys(row)) vs $(names)"))
	end
	DBInterface.execute(dbc.conn, "insert into $qtblname values ($(join(fill("?", length(names)), ", ")))", (row[collect(names)]...,))
	return dbc
end

function Base.copy!(dbc::SQLCollection, rows)
	tblname = _tablename(dbc)
	_copy_impl!(dbc.conn.raw, rows, tblname, dbc.conn.catalog.dialect)
	if !haskey(dbc.conn.catalog.tables, tblname)
		new_cat = FunSQL.reflect(dbc.conn.raw; dbc.conn.catalog.dialect)
		dbc.conn.catalog.tables[tblname] = new_cat.tables[tblname]
	end
	return dbc
end

function _copy_impl! end

function Base.empty!(dbc::SQLCollection)
	qtblname = _quote_ident(_tablename(dbc), dbc.conn)
	DBInterface.execute(dbc.conn, "delete from $qtblname")
	return dbc
end

function drop!(dbc::SQLCollection)
	tblname = _tablename(dbc)
	qtblname = _quote_ident(tblname, dbc.conn)
	DBInterface.execute(dbc.conn, "drop table $qtblname")
	delete!(dbc.conn.catalog.tables, tblname)
	return dbc
end
