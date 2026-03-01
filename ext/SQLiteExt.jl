module SQLiteExt

using SQLite
import SQLCollections: _copy_impl!, _create_impl!, _quote_ident
using SQLCollections: @p

function _copy_impl!(conn::SQLite.DB, rows, tblname::Symbol, dialect)
    qi(x) = _quote_ident(x, dialect)
    if !isnothing(SQLite.tableinfo(conn, string(tblname)))
        DBInterface.execute(conn, "DELETE FROM $(qi(tblname))")
    end
    SQLite.load!(rows, conn, string(tblname); ifnotexists=false)
end

_create_impl!(conn::SQLite.DB, tblname::Symbol, dialect, T::Type) = SQLite.createtable!(conn, string(tblname), SQLite.Tables.Schema(T))

function _create_impl!(conn::SQLite.DB, tblname::Symbol, dialect, Tk::Type{<:NamedTuple}, Tv::Type{<:NamedTuple})
    qi(x) = _quote_ident(x, dialect)
    names = (fieldnames(Tk)..., fieldnames(Tv)...)
    types = (fieldtypes(Tk)..., fieldtypes(Tv)...)
    coldefs = names .=> SQLite.sqlitetype.(types)
    SQLite.execute(conn,
    """CREATE TABLE $(qi(tblname)) (
        $(@p coldefs map("$(qi(_[1])) $(_[2])") join(__, ", ")),
        PRIMARY KEY ($(join(qi.(fieldnames(Tk)), ", ")))
    ) STRICT""")
end

end
