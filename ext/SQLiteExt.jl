module SQLiteExt

using SQLite
import DBCollections: _copy_impl!

_copy_impl!(conn::SQLite.DB, rows, tblname::Symbol) = SQLite.load!(rows, conn, string(tblname); ifnotexists=false)

end
