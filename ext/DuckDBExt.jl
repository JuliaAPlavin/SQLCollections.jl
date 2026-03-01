module DuckDBExt

using DuckDB
using Tables
using FunSQL: SQLTable, From
import SQLCollections: SQLCollection, _copy_impl!, _quote_ident, _tablename, rowtable, _register_virtual, _unregister_virtual

function _copy_impl!(conn::DuckDB.DB, rows, tblname::Symbol, dialect)
	qi(x) = _quote_ident(x, dialect)
	tmp_tblname = String(rand('a':'z', 50))
	DuckDB.register_table(conn, rows, tmp_tblname)
	try
		tbls = DBInterface.execute(conn, "SELECT 1 FROM information_schema.tables WHERE table_schema = 'main' AND table_name = '$(string(tblname))'") |> rowtable
		if isempty(tbls)
			DBInterface.execute(conn, "CREATE TABLE $(qi(tblname)) AS FROM $tmp_tblname")
		else
			@assert length(tbls) == 1
			DBInterface.execute(conn, "DELETE FROM $(qi(tblname))")
			DBInterface.execute(conn, "INSERT INTO $(qi(tblname)) FROM $tmp_tblname")
		end
	finally
		DuckDB.unregister_table(conn, tmp_tblname)
	end
end

function _register_virtual(conn, data)
	conn.raw isa DuckDB.DB || error("_register_virtual is only supported for DuckDB connections")
	name = Symbol(join(rand('a':'z', 30)))
	DuckDB.register_table(conn.raw, data, String(name))
	col_names = collect(map(Symbol, Tables.columnnames(Tables.columns(data))))
	conn.catalog.tables[name] = SQLTable(name=name, columns=col_names)
	return SQLCollection(conn, From(name))
end

function _unregister_virtual(dbc::SQLCollection)
	name = _tablename(dbc)
	DuckDB.unregister_table(dbc.conn.raw, String(name))
	delete!(dbc.conn.catalog.tables, name)
end

end
