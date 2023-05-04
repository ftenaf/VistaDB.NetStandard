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
      _table = null;
    }

    internal VistaDBDataReader DequeueReader()
    {
      return Dequeue();
    }

    public void Send(VistaDBDataReader reader)
    {
      if (reader == null)
        return;
      Enqueue(reader);
    }

    public void Send(string message)
    {
      Send(new VistaDBDataReader(VistaDBContext.SQLChannel.CurrentConnection.CreateMessageQuery(message), null, CommandBehavior.SingleRow));
    }

    internal void Send(TemporaryResultSet table)
    {
      Send(new VistaDBDataReader(VistaDBContext.SQLChannel.CurrentConnection.CreateResultQuery(table), null, CommandBehavior.SingleResult));
    }

    public void Send(SqlDataRecord record)
    {
      Send(new VistaDBDataReader(VistaDBContext.SQLChannel.CurrentConnection.CreateResultQuery(record.DataTable), null, CommandBehavior.SingleResult));
    }

    public void SendResultsStart(SqlDataRecord record)
    {
      if (_table != null)
        Send(_table);
      _table = record.DataTable;
    }

    public void SendResultsEnd()
    {
      Send(_table);
      _table = null;
    }

    public void SendResultsRow(SqlDataRecord record)
    {
      if (_table != record.DataTable)
      {
        SendResultsEnd();
        SendResultsStart(record);
      }
      _table.Post();
      _table.Insert();
    }

    public void ExecuteAndSend(VistaDBCommand command)
    {
      Send(command.ExecuteReader());
      command.Dispose();
    }
  }
}
