module DuckDBExt

using DuckDB
import DBCollections: _copy_impl!

function _copy_impl!(conn::DuckDB.DB, rows, tblname::Symbol)
	tmp_tblname = String(rand('a':'z', 50))
	DuckDB.register_table(conn, rows, tmp_tblname)
	try
		# DBInterface.execute(conn, "DELETE FROM $tblname")
		# DBInterface.execute(conn, "INSERT INTO $tblname FROM $tmp_tblname")
		DBInterface.execute(conn, "CREATE TABLE $tblname AS FROM $tmp_tblname")
	finally
		DuckDB.unregister_table(conn, tmp_tblname)
	end
end

end
