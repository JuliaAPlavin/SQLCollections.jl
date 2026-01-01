module DuckDBExt

using DuckDB
import SQLCollections: _copy_impl!, rowtable

# XXX: should be upstreamed in https://github.com/duckdb/duckdb/pull/17585
DuckDB.DB(file; readonly::Bool=false) =
	if readonly
		cnf = DuckDB.Config()
		DuckDB.set_config(cnf, "access_mode", "READ_ONLY")
		DuckDB.DB(file, cnf)
	else
		DuckDB.DB(file)
	end

function _copy_impl!(conn::DuckDB.DB, rows, tblname::Symbol)
	tmp_tblname = String(rand('a':'z', 50))
	DuckDB.register_table(conn, rows, tmp_tblname)
	try
		tbls = DBInterface.execute(conn, "SELECT 1 FROM information_schema.tables WHERE table_schema = 'main' AND table_name = '$tblname'") |> rowtable
		if isempty(tbls)
			DBInterface.execute(conn, "CREATE TABLE $tblname AS FROM $tmp_tblname")
		else
			@assert length(tbls) == 1
			DBInterface.execute(conn, "DELETE FROM $tblname")
			DBInterface.execute(conn, "INSERT INTO $tblname FROM $tmp_tblname")
		end
	finally
		DuckDB.unregister_table(conn, tmp_tblname)
	end
end

end
