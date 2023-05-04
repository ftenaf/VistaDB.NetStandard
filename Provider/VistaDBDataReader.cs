using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL;

namespace VistaDB.Provider
{
  public sealed class VistaDBDataReader : DbDataReader, IEnumerator
  {
    private VistaDBPipe pipe = new VistaDBPipe();
    private IQueryStatement queryStatements;
    private VistaDBConnection vdbConnection;
    private bool closeConnection;
    private IQueryResult queryResult;
    private IQuerySchemaInfo queryResultSchema;
    private bool schemaOnly;
    private bool singleResult;
    private bool singleRow;
    private long affectedRows;
    private bool first;
    private VistaDBDataReader pipedReader;

    object IEnumerator.Current
    {
      get
      {
        return this;
      }
    }

    bool IEnumerator.MoveNext()
    {
      return Read();
    }

    void IEnumerator.Reset()
    {
      queryResult.FirstRow();
      first = true;
    }

    internal VistaDBDataReader(IQueryStatement statement, VistaDBConnection connection, CommandBehavior commandBehavior)
    {
      vdbConnection = connection;
      closeConnection = (commandBehavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection;
      schemaOnly = (commandBehavior & CommandBehavior.SchemaOnly) == CommandBehavior.SchemaOnly;
      singleResult = (commandBehavior & CommandBehavior.SingleResult) == CommandBehavior.SingleResult;
      singleRow = (commandBehavior & CommandBehavior.SingleRow) == CommandBehavior.SingleRow;
      InitStatement(statement);
      GoNextResult();
      LockQuery();
    }

    private VistaDBDataReader(IQueryStatement statement)
    {
      InitStatement(statement);
    }

    private void InitStatement(IQueryStatement statement)
    {
      queryStatements = statement;
      statement.ResetResult();
    }

    internal static VistaDBDataReader NonQueryReader(IQueryStatement statement)
    {
      return new VistaDBDataReader(statement);
    }

    public override int Depth
    {
      get
      {
        return 0;
      }
    }

    public override int FieldCount
    {
      get
      {
        if (queryResultSchema != null)
          return queryResultSchema.ColumnCount;
        return 0;
      }
    }

    public override bool HasRows
    {
      get
      {
        if (queryResult != null)
          return queryResult.RowCount > 0L;
        return false;
      }
    }

    public override bool IsClosed
    {
      get
      {
        if (queryResult == null && queryResultSchema == null)
          return !schemaOnly;
        return false;
      }
    }

    public override object this[int ordinal]
    {
      get
      {
        return queryResult.GetValue(ordinal, VistaDBType.Unknown);
      }
    }

    public override object this[string name]
    {
      get
      {
        return queryResult.GetValue(GetOrdinal(name), VistaDBType.Unknown);
      }
    }

    public override int RecordsAffected
    {
      get
      {
        return (int) affectedRows;
      }
    }

    public override void Close()
    {
      lock (this)
      {
        UnlockQuery();
        if (pipedReader != null)
          pipedReader.Close();
        if (pipe != null)
          pipe.Clear();
        try
        {
          if (queryResult != null)
            queryResult.Close();
          if (vdbConnection == null)
            return;
          if (queryStatements != null)
            vdbConnection.FreeQuery(queryStatements, true);
          if (!closeConnection)
            return;
          vdbConnection.Close();
        }
        finally
        {
          queryStatements = null;
          queryResult = null;
          queryResultSchema = null;
          vdbConnection = null;
        }
      }
    }

    public override bool GetBoolean(int ordinal)
    {
      object obj = queryResult.GetValue(ordinal, VistaDBType.Bit);
      if (obj != null)
        return (bool) obj;
      return false;
    }

    public override byte GetByte(int ordinal)
    {
      object obj = queryResult.GetValue(ordinal, VistaDBType.TinyInt);
      if (obj != null)
        return (byte) obj;
      return 0;
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
    {
      Array sourceArray = (Array) queryResult.GetValue(ordinal, VistaDBType.Image);
      if (sourceArray == null || sourceArray.Length == 0)
        return 0;
      if (buffer == null)
        return sourceArray.Length;
      long length1 = sourceArray.Length - dataOffset;
      if (length1 > length)
        length1 = length;
      Array.Copy(sourceArray, dataOffset, buffer, bufferOffset, length1);
      return length1;
    }

    public override char GetChar(int ordinal)
    {
      string str = (string) queryResult.GetValue(ordinal, VistaDBType.NChar);
      if (str != null && str.Length != 0)
        return str[0];
      return char.MinValue;
    }

    public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
    {
      string str = (string) queryResult.GetValue(ordinal, VistaDBType.NChar);
      if (str == null || str.Length == 0)
        return 0;
      if (buffer == null)
        return str.Length;
      long length1 = str.Length - dataOffset;
      if (length1 > length)
        length1 = length;
      Array.Copy(str.ToCharArray(), dataOffset, buffer, bufferOffset, length1);
      return length1;
    }

    public override string GetDataTypeName(int ordinal)
    {
      return queryResultSchema.GetDataTypeName(ordinal);
    }

    public override DateTime GetDateTime(int ordinal)
    {
      object obj = queryResult.GetValue(ordinal, VistaDBType.DateTime);
      if (obj != null)
        return (DateTime) obj;
      return DateTime.MinValue;
    }

    public override Decimal GetDecimal(int ordinal)
    {
      object obj = queryResult.GetValue(ordinal, VistaDBType.Decimal);
      if (obj != null)
        return (Decimal) obj;
      return new Decimal(0);
    }

    public override double GetDouble(int ordinal)
    {
      object obj = queryResult.GetValue(ordinal, VistaDBType.Float);
      if (obj != null)
        return (double) obj;
      return 0.0;
    }

    public override IEnumerator GetEnumerator()
    {
      return new DbEnumerator((IDataReader)this, closeConnection);
    }

    public override Type GetFieldType(int ordinal)
    {
      return queryResultSchema.GetColumnType(ordinal);
    }

    public VistaDBType GetFieldVistaDBType(int ordinal)
    {
      return queryResultSchema.GetColumnVistaDBType(ordinal);
    }

    public override float GetFloat(int ordinal)
    {
      object obj = queryResult.GetValue(ordinal, VistaDBType.Real);
      if (obj != null)
        return (float) obj;
      return 0.0f;
    }

    public override Guid GetGuid(int ordinal)
    {
      object obj = queryResult.GetValue(ordinal, VistaDBType.UniqueIdentifier);
      if (obj != null)
        return (Guid) obj;
      return Guid.Empty;
    }

    public override short GetInt16(int ordinal)
    {
      object obj = queryResult.GetValue(ordinal, VistaDBType.SmallInt);
      if (obj != null)
        return (short) obj;
      return 0;
    }

    public override int GetInt32(int ordinal)
    {
      object obj = queryResult.GetValue(ordinal, VistaDBType.Int);
      if (obj != null)
        return (int) obj;
      return 0;
    }

    public override long GetInt64(int ordinal)
    {
      object obj = queryResult.GetValue(ordinal, VistaDBType.BigInt);
      if (obj != null)
        return (long) obj;
      return 0;
    }

    public override string GetName(int ordinal)
    {
      return queryResultSchema.GetAliasName(ordinal);
    }

    public override int GetOrdinal(string name)
    {
      return queryResultSchema.GetColumnOrdinal(name);
    }

    public override DataTable GetSchemaTable()
    {
      return queryResultSchema.GetSchemaTable();
    }

    public override string GetString(int ordinal)
    {
      return (string) queryResult.GetValue(ordinal, VistaDBType.NChar);
    }

    public override object GetValue(int ordinal)
    {
      return queryResult.GetValue(ordinal, VistaDBType.Unknown) ?? Convert.DBNull;
    }

    public override int GetValues(object[] values)
    {
      int num = values.Length < FieldCount ? values.Length : FieldCount;
      for (int index = 0; index < num; ++index)
      {
        object obj = queryResult.GetValue(index, VistaDBType.Unknown);
        values[index] = obj == null ? Convert.DBNull : obj;
      }
      return num;
    }

    public override bool IsDBNull(int ordinal)
    {
      return queryResult.IsNull(ordinal);
    }

    private bool GoNextResult()
    {
      if (queryResult != null)
        queryResult.Close();
      queryResult = null;
      queryResultSchema = null;
      affectedRows = -1L;
      if (pipedReader == null)
      {
        if (pipe.Count == 0)
        {
          for (INextQueryResult nextQueryResult = queryStatements.NextResult(pipe); nextQueryResult != null; nextQueryResult = queryStatements.NextResult(pipe))
          {
            if (nextQueryResult.AffectedRows > 0L)
            {
              if (affectedRows < 0L)
                ++affectedRows;
              affectedRows += nextQueryResult.AffectedRows;
            }
            if (pipe.Count > 0 && GoNextResult())
              return true;
            if (nextQueryResult.ResultSet != null)
            {
              queryResultSchema = nextQueryResult.Schema;
              if (schemaOnly)
              {
                if (nextQueryResult.ResultSet != null)
                {
                  nextQueryResult.ResultSet.Close();
                  break;
                }
                break;
              }
              queryResult = nextQueryResult.ResultSet;
              break;
            }
          }
          first = queryResult != null;
          if (queryResult == null)
            return queryResultSchema != null;
          return true;
        }
        pipedReader = pipe.DequeueReader();
      }
      else if (!pipedReader.GoNextResult())
      {
        pipedReader.Close();
        pipedReader = null;
        return GoNextResult();
      }
      affectedRows = pipedReader.affectedRows;
      queryResult = pipedReader.queryResult;
      queryResultSchema = pipedReader.queryResultSchema;
      first = queryResult != null;
      if (queryResult == null)
        return queryResultSchema != null;
      return true;
    }

    public override bool NextResult()
    {
      if (queryResult == null && queryResultSchema == null)
        return false;
      return GoNextResult();
    }

    public override bool Read()
    {
      if (queryResult == null)
        return false;
      if (first)
      {
        first = false;
        return !queryResult.EndOfTable;
      }
      queryResult.NextRow();
      return !queryResult.EndOfTable;
    }

    private void EvaluateScalar(IQueryResult resultSet, ref object scalar)
    {
      if (resultSet == null)
        return;
      try
      {
        if (scalar != null)
          return;
        resultSet.FirstRow();
        scalar = resultSet.GetValue(0, VistaDBType.Unknown);
      }
      finally
      {
        resultSet.Close();
      }
    }

    internal object ExecQuery(ref long affectedRows)
    {
      object scalar1 = null;
      if (pipedReader == null)
      {
        if (pipe.Count == 0)
        {
          object scalar2 = null;
          for (INextQueryResult nextQueryResult = queryStatements.NextResult(pipe); nextQueryResult != null; nextQueryResult = queryStatements.NextResult(pipe))
          {
            affectedRows += nextQueryResult.AffectedRows;
            if (pipe.Count > 0)
            {
              object obj = ExecQuery(ref affectedRows);
              if (scalar2 == null)
                scalar2 = obj;
            }
            EvaluateScalar(nextQueryResult.ResultSet, ref scalar2);
          }
          return scalar2;
        }
        pipedReader = pipe.DequeueReader();
        EvaluateScalar(pipedReader.queryResult, ref scalar1);
      }
      else
      {
        scalar1 = pipedReader.ExecQuery(ref affectedRows);
        pipedReader.Close();
        pipedReader = null;
      }
      return scalar1;
    }

    internal INextQueryResult CurrentResult
    {
      get
      {
        return new BatchStatement.ResultSetData(queryResult, queryResultSchema, affectedRows);
      }
    }

    private void LockQuery()
    {
      if (queryStatements == null)
        return;
      queryStatements.LockedDisposing = true;
    }

    private void UnlockQuery()
    {
      if (queryStatements == null)
        return;
      queryStatements.LockedDisposing = false;
    }
  }
}
