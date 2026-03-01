module DuckDBExt

using DuckDB
import SQLCollections: _copy_impl!, _quote_ident, rowtable

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

end
