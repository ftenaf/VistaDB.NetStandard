using System.Data;
using System.Data.Common;

namespace VistaDB.Provider
{
  public sealed class VistaDBRowUpdatedEventArgs : RowUpdatedEventArgs
  {
    public VistaDBRowUpdatedEventArgs(DataRow row, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
      : base(row, command, statementType, tableMapping)
    {
    }

    public VistaDBCommand Command
    {
      get
      {
        return (VistaDBCommand) base.Command;
      }
    }
  }
}
