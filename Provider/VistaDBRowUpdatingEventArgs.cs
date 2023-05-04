using System.Data;
using System.Data.Common;

namespace VistaDB.Provider
{
  public sealed class VistaDBRowUpdatingEventArgs : RowUpdatingEventArgs
  {
    public VistaDBRowUpdatingEventArgs(DataRow row, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
      : base(row, command, statementType, tableMapping)
    {
    }

    protected override IDbCommand BaseCommand
    {
      get
      {
        return base.BaseCommand;
      }
      set
      {
        base.BaseCommand = value;
      }
    }

    new public VistaDBCommand Command
    {
      get
      {
        return (VistaDBCommand) base.Command;
      }
      set
      {
        Command = value;
      }
    }
  }
}
