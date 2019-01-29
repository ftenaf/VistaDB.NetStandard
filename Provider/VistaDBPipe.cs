using System.Collections.Generic;
using System.Data;
using VistaDB.Compatibility.SqlServer;
using VistaDB.Engine.SQL;

namespace VistaDB.Provider
{
  public sealed class VistaDBPipe : Queue<VistaDBDataReader>
  {
    private TemporaryResultSet _table;

    internal VistaDBPipe()
    {
      this._table = (TemporaryResultSet) null;
    }

    internal VistaDBDataReader DequeueReader()
    {
      return this.Dequeue();
    }

    public void Send(VistaDBDataReader reader)
    {
      if (reader == null)
        return;
      this.Enqueue(reader);
    }

    public void Send(string message)
    {
      this.Send(new VistaDBDataReader(VistaDBContext.SQLChannel.CurrentConnection.CreateMessageQuery(message), (VistaDBConnection) null, CommandBehavior.SingleRow));
    }

    internal void Send(TemporaryResultSet table)
    {
      this.Send(new VistaDBDataReader(VistaDBContext.SQLChannel.CurrentConnection.CreateResultQuery(table), (VistaDBConnection) null, CommandBehavior.SingleResult));
    }

    public void Send(SqlDataRecord record)
    {
      this.Send(new VistaDBDataReader(VistaDBContext.SQLChannel.CurrentConnection.CreateResultQuery(record.DataTable), (VistaDBConnection) null, CommandBehavior.SingleResult));
    }

    public void SendResultsStart(SqlDataRecord record)
    {
      if (this._table != null)
        this.Send(this._table);
      this._table = record.DataTable;
    }

    public void SendResultsEnd()
    {
      this.Send(this._table);
      this._table = (TemporaryResultSet) null;
    }

    public void SendResultsRow(SqlDataRecord record)
    {
      if (this._table != record.DataTable)
      {
        this.SendResultsEnd();
        this.SendResultsStart(record);
      }
      this._table.Post();
      this._table.Insert();
    }

    public void ExecuteAndSend(VistaDBCommand command)
    {
      this.Send(command.ExecuteReader());
      command.Dispose();
    }
  }
}
