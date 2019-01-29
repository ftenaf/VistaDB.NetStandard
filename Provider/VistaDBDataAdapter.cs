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
      : base((DbDataAdapter) adapter)
    {
    }

    public VistaDBDataAdapter(VistaDBCommand selectCommand)
      : this()
    {
      this.SelectCommand = selectCommand;
    }

    public VistaDBDataAdapter(string selectCommandText, VistaDBConnection conn)
      : this()
    {
      this.SelectCommand = new VistaDBCommand(selectCommandText, conn);
    }

    public VistaDBDataAdapter(string selectCommandText, string connectionString)
      : this()
    {
      VistaDBConnection connection = new VistaDBConnection(connectionString);
      this.SelectCommand = new VistaDBCommand(selectCommandText, connection);
    }

    public VistaDBCommand DeleteCommand
    {
      get
      {
        return this.cmdDelete;
      }
      set
      {
        this.cmdDelete = value;
      }
    }

    public VistaDBCommand InsertCommand
    {
      get
      {
        return this.cmdInsert;
      }
      set
      {
        this.cmdInsert = value;
      }
    }

    public VistaDBCommand SelectCommand
    {
      get
      {
        return this.cmdSelect;
      }
      set
      {
        this.cmdSelect = value;
      }
    }

    public VistaDBCommand UpdateCommand
    {
      get
      {
        return this.cmdUpdate;
      }
      set
      {
        this.cmdUpdate = value;
      }
    }

    protected override RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
    {
      return (RowUpdatedEventArgs) new VistaDBRowUpdatedEventArgs(dataRow, command, statementType, tableMapping);
    }

    protected override RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
    {
      return (RowUpdatingEventArgs) new VistaDBRowUpdatingEventArgs(dataRow, command, statementType, tableMapping);
    }

    protected override void OnRowUpdated(RowUpdatedEventArgs value)
    {
      VistaDBRowUpdatedEventHandler updatedEventHandler = (VistaDBRowUpdatedEventHandler) this.Events[VistaDBDataAdapter.EventRowUpdated];
      if (updatedEventHandler != null && value is VistaDBRowUpdatedEventArgs)
        updatedEventHandler((object) this, (VistaDBRowUpdatedEventArgs) value);
      base.OnRowUpdated(value);
    }

    protected override void OnRowUpdating(RowUpdatingEventArgs value)
    {
      VistaDBRowUpdatingEventHandler updatingEventHandler = (VistaDBRowUpdatingEventHandler) this.Events[VistaDBDataAdapter.EventRowUpdating];
      if (updatingEventHandler != null && value is VistaDBRowUpdatingEventArgs)
        updatingEventHandler((object) this, (VistaDBRowUpdatingEventArgs) value);
      base.OnRowUpdating(value);
    }

    public event VistaDBRowUpdatingEventHandler RowUpdating
    {
      add
      {
        VistaDBRowUpdatingEventHandler updatingEventHandler = (VistaDBRowUpdatingEventHandler) this.Events[VistaDBDataAdapter.EventRowUpdating];
        if (updatingEventHandler != null && value.Target is DbCommandBuilder)
        {
          VistaDBRowUpdatingEventHandler builder = (VistaDBRowUpdatingEventHandler) VistaDBCommandBuilder.FindBuilder((MulticastDelegate) updatingEventHandler);
          if (builder != null)
            this.Events.RemoveHandler(VistaDBDataAdapter.EventRowUpdating, (Delegate) builder);
        }
        this.Events.AddHandler(VistaDBDataAdapter.EventRowUpdating, (Delegate) value);
      }
      remove
      {
        this.Events.RemoveHandler(VistaDBDataAdapter.EventRowUpdating, (Delegate) value);
      }
    }

    public event VistaDBRowUpdatedEventHandler RowUpdated
    {
      add
      {
        this.Events.AddHandler(VistaDBDataAdapter.EventRowUpdated, (Delegate) value);
      }
      remove
      {
        this.Events.RemoveHandler(VistaDBDataAdapter.EventRowUpdated, (Delegate) value);
      }
    }

    IDbCommand IDbDataAdapter.DeleteCommand
    {
      get
      {
        return (IDbCommand) this.cmdDelete;
      }
      set
      {
        this.cmdDelete = (VistaDBCommand) value;
      }
    }

    IDbCommand IDbDataAdapter.InsertCommand
    {
      get
      {
        return (IDbCommand) this.cmdInsert;
      }
      set
      {
        this.cmdInsert = (VistaDBCommand) value;
      }
    }

    IDbCommand IDbDataAdapter.SelectCommand
    {
      get
      {
        return (IDbCommand) this.cmdSelect;
      }
      set
      {
        this.cmdSelect = (VistaDBCommand) value;
      }
    }

    IDbCommand IDbDataAdapter.UpdateCommand
    {
      get
      {
        return (IDbCommand) this.cmdUpdate;
      }
      set
      {
        this.cmdUpdate = (VistaDBCommand) value;
      }
    }

    object ICloneable.Clone()
    {
      return (object) new VistaDBDataAdapter(this);
    }
  }
}
