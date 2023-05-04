using System;
using System.Data;
using System.Data.Common;

namespace VistaDB.Provider
{
  public sealed class VistaDBDataAdapter : DbDataAdapter, IDbDataAdapter, IDataAdapter, ICloneable
  {
    private static readonly object EventRowUpdated = new object();
    private static readonly object EventRowUpdating = new object();
    private VistaDBCommand cmdInsert;
    private VistaDBCommand cmdDelete;
    private VistaDBCommand cmdSelect;
    private VistaDBCommand cmdUpdate;

    public VistaDBDataAdapter()
    {
    }

    private VistaDBDataAdapter(VistaDBDataAdapter adapter)
      : base(adapter)
    {
    }

    public VistaDBDataAdapter(VistaDBCommand selectCommand)
      : this()
    {
      SelectCommand = selectCommand;
    }

    public VistaDBDataAdapter(string selectCommandText, VistaDBConnection conn)
      : this()
    {
      SelectCommand = new VistaDBCommand(selectCommandText, conn);
    }

    public VistaDBDataAdapter(string selectCommandText, string connectionString)
      : this()
    {
      VistaDBConnection connection = new VistaDBConnection(connectionString);
      SelectCommand = new VistaDBCommand(selectCommandText, connection);
    }

    public new VistaDBCommand DeleteCommand
    {
      get
      {
        return cmdDelete;
      }
      set
      {
        cmdDelete = value;
      }
    }

    new public VistaDBCommand InsertCommand
    {
      get
      {
        return cmdInsert;
      }
      set
      {
        cmdInsert = value;
      }
    }

    public new VistaDBCommand SelectCommand
    {
      get
      {
        return cmdSelect;
      }
      set
      {
        cmdSelect = value;
      }
    }

    public new VistaDBCommand UpdateCommand
    {
      get
      {
        return cmdUpdate;
      }
      set
      {
        cmdUpdate = value;
      }
    }

    protected override RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
    {
      return new VistaDBRowUpdatedEventArgs(dataRow, command, statementType, tableMapping);
    }

    protected override RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
    {
      return new VistaDBRowUpdatingEventArgs(dataRow, command, statementType, tableMapping);
    }

    protected override void OnRowUpdated(RowUpdatedEventArgs value)
    {
      VistaDBRowUpdatedEventHandler updatedEventHandler = (VistaDBRowUpdatedEventHandler) Events[EventRowUpdated];
      if (updatedEventHandler != null && value is VistaDBRowUpdatedEventArgs)
        updatedEventHandler(this, (VistaDBRowUpdatedEventArgs) value);
      base.OnRowUpdated(value);
    }

    protected override void OnRowUpdating(RowUpdatingEventArgs value)
    {
      VistaDBRowUpdatingEventHandler updatingEventHandler = (VistaDBRowUpdatingEventHandler) Events[EventRowUpdating];
      if (updatingEventHandler != null && value is VistaDBRowUpdatingEventArgs)
        updatingEventHandler(this, (VistaDBRowUpdatingEventArgs) value);
      base.OnRowUpdating(value);
    }

    public event VistaDBRowUpdatingEventHandler RowUpdating
    {
      add
      {
        VistaDBRowUpdatingEventHandler updatingEventHandler = (VistaDBRowUpdatingEventHandler) Events[EventRowUpdating];
        if (updatingEventHandler != null && value.Target is DbCommandBuilder)
        {
          VistaDBRowUpdatingEventHandler builder = (VistaDBRowUpdatingEventHandler) VistaDBCommandBuilder.FindBuilder(updatingEventHandler);
          if (builder != null)
            Events.RemoveHandler(EventRowUpdating, builder);
        }
        Events.AddHandler(EventRowUpdating, value);
      }
      remove
      {
        Events.RemoveHandler(EventRowUpdating, value);
      }
    }

    public event VistaDBRowUpdatedEventHandler RowUpdated
    {
      add
      {
        Events.AddHandler(EventRowUpdated, value);
      }
      remove
      {
        Events.RemoveHandler(EventRowUpdated, value);
      }
    }

    IDbCommand IDbDataAdapter.DeleteCommand
    {
      get
      {
        return cmdDelete;
      }
      set
      {
        cmdDelete = (VistaDBCommand) value;
      }
    }

    IDbCommand IDbDataAdapter.InsertCommand
    {
      get
      {
        return cmdInsert;
      }
      set
      {
        cmdInsert = (VistaDBCommand) value;
      }
    }

    IDbCommand IDbDataAdapter.SelectCommand
    {
      get
      {
        return cmdSelect;
      }
      set
      {
        cmdSelect = (VistaDBCommand) value;
      }
    }

    IDbCommand IDbDataAdapter.UpdateCommand
    {
      get
      {
        return cmdUpdate;
      }
      set
      {
        cmdUpdate = (VistaDBCommand) value;
      }
    }

    object ICloneable.Clone()
    {
      return new VistaDBDataAdapter(this);
    }
  }
}
