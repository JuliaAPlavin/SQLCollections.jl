module SQLiteExt

using SQLite
import SQLCollections: _copy_impl!, _create_impl!
using SQLCollections: @p

_copy_impl!(conn::SQLite.DB, rows, tblname::Symbol) = SQLite.load!(rows, conn, string(tblname); ifnotexists=false)

_create_impl!(conn::SQLite.DB, tblname::Symbol, T::Type) = SQLite.createtable!(conn, string(tblname), SQLite.Tables.Schema(T))

function _create_impl!(conn::SQLite.DB, tblname::Symbol, Tk::Type{<:NamedTuple}, Tv::Type{<:NamedTuple})
    names = (fieldnames(Tk)..., fieldnames(Tv)...)
    types = (fieldtypes(Tk)..., fieldtypes(Tv)...)
    coldefs = names .=> SQLite.sqlitetype.(types)
    SQLite.execute(conn,
    """CREATE TABLE $tblname (
        $(@p coldefs map("$(_[1]) $(_[2])") join(__, ", ")),
        PRIMARY KEY ($(join(fieldnames(Tk), ", ")))
    ) STRICT""")
end

end
