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
        return (object) this;
      }
    }

    bool IEnumerator.MoveNext()
    {
      return this.Read();
    }

    void IEnumerator.Reset()
    {
      this.queryResult.FirstRow();
      this.first = true;
    }

    internal VistaDBDataReader(IQueryStatement statement, VistaDBConnection connection, CommandBehavior commandBehavior)
    {
      this.vdbConnection = connection;
      this.closeConnection = (commandBehavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection;
      this.schemaOnly = (commandBehavior & CommandBehavior.SchemaOnly) == CommandBehavior.SchemaOnly;
      this.singleResult = (commandBehavior & CommandBehavior.SingleResult) == CommandBehavior.SingleResult;
      this.singleRow = (commandBehavior & CommandBehavior.SingleRow) == CommandBehavior.SingleRow;
      this.InitStatement(statement);
      this.GoNextResult();
      this.LockQuery();
    }

    private VistaDBDataReader(IQueryStatement statement)
    {
      this.InitStatement(statement);
    }

    private void InitStatement(IQueryStatement statement)
    {
      this.queryStatements = statement;
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
        if (this.queryResultSchema != null)
          return this.queryResultSchema.ColumnCount;
        return 0;
      }
    }

    public override bool HasRows
    {
      get
      {
        if (this.queryResult != null)
          return this.queryResult.RowCount > 0L;
        return false;
      }
    }

    public override bool IsClosed
    {
      get
      {
        if (this.queryResult == null && this.queryResultSchema == null)
          return !this.schemaOnly;
        return false;
      }
    }

    public override object this[int ordinal]
    {
      get
      {
        return this.queryResult.GetValue(ordinal, VistaDBType.Unknown);
      }
    }

    public override object this[string name]
    {
      get
      {
        return this.queryResult.GetValue(this.GetOrdinal(name), VistaDBType.Unknown);
      }
    }

    public override int RecordsAffected
    {
      get
      {
        return (int) this.affectedRows;
      }
    }

    public override void Close()
    {
      lock (this)
      {
        this.UnlockQuery();
        if (this.pipedReader != null)
          this.pipedReader.Close();
        if (this.pipe != null)
          this.pipe.Clear();
        try
        {
          if (this.queryResult != null)
            this.queryResult.Close();
          if (this.vdbConnection == null)
            return;
          if (this.queryStatements != null)
            this.vdbConnection.FreeQuery(this.queryStatements, true);
          if (!this.closeConnection)
            return;
          this.vdbConnection.Close();
        }
        finally
        {
          this.queryStatements = (IQueryStatement) null;
          this.queryResult = (IQueryResult) null;
          this.queryResultSchema = (IQuerySchemaInfo) null;
          this.vdbConnection = (VistaDBConnection) null;
        }
      }
    }

    public override bool GetBoolean(int ordinal)
    {
      object obj = this.queryResult.GetValue(ordinal, VistaDBType.Bit);
      if (obj != null)
        return (bool) obj;
      return false;
    }

    public override byte GetByte(int ordinal)
    {
      object obj = this.queryResult.GetValue(ordinal, VistaDBType.TinyInt);
      if (obj != null)
        return (byte) obj;
      return 0;
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
    {
      Array sourceArray = (Array) this.queryResult.GetValue(ordinal, VistaDBType.Image);
      if (sourceArray == null || sourceArray.Length == 0)
        return 0;
      if (buffer == null)
        return (long) sourceArray.Length;
      long length1 = (long) sourceArray.Length - dataOffset;
      if (length1 > (long) length)
        length1 = (long) length;
      Array.Copy(sourceArray, dataOffset, (Array) buffer, (long) bufferOffset, length1);
      return length1;
    }

    public override char GetChar(int ordinal)
    {
      string str = (string) this.queryResult.GetValue(ordinal, VistaDBType.NChar);
      if (str != null && str.Length != 0)
        return str[0];
      return char.MinValue;
    }

    public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
    {
      string str = (string) this.queryResult.GetValue(ordinal, VistaDBType.NChar);
      if (str == null || str.Length == 0)
        return 0;
      if (buffer == null)
        return (long) str.Length;
      long length1 = (long) str.Length - dataOffset;
      if (length1 > (long) length)
        length1 = (long) length;
      Array.Copy((Array) str.ToCharArray(), dataOffset, (Array) buffer, (long) bufferOffset, length1);
      return length1;
    }

    public override string GetDataTypeName(int ordinal)
    {
      return this.queryResultSchema.GetDataTypeName(ordinal);
    }

    public override DateTime GetDateTime(int ordinal)
    {
      object obj = this.queryResult.GetValue(ordinal, VistaDBType.DateTime);
      if (obj != null)
        return (DateTime) obj;
      return DateTime.MinValue;
    }

    public override Decimal GetDecimal(int ordinal)
    {
      object obj = this.queryResult.GetValue(ordinal, VistaDBType.Decimal);
      if (obj != null)
        return (Decimal) obj;
      return new Decimal(0);
    }

    public override double GetDouble(int ordinal)
    {
      object obj = this.queryResult.GetValue(ordinal, VistaDBType.Float);
      if (obj != null)
        return (double) obj;
      return 0.0;
    }

    public override IEnumerator GetEnumerator()
    {
      return (IEnumerator) new DbEnumerator((IDataReader) this, this.closeConnection);
    }

    public override Type GetFieldType(int ordinal)
    {
      return this.queryResultSchema.GetColumnType(ordinal);
    }

    public VistaDBType GetFieldVistaDBType(int ordinal)
    {
      return this.queryResultSchema.GetColumnVistaDBType(ordinal);
    }

    public override float GetFloat(int ordinal)
    {
      object obj = this.queryResult.GetValue(ordinal, VistaDBType.Real);
      if (obj != null)
        return (float) obj;
      return 0.0f;
    }

    public override Guid GetGuid(int ordinal)
    {
      object obj = this.queryResult.GetValue(ordinal, VistaDBType.UniqueIdentifier);
      if (obj != null)
        return (Guid) obj;
      return Guid.Empty;
    }

    public override short GetInt16(int ordinal)
    {
      object obj = this.queryResult.GetValue(ordinal, VistaDBType.SmallInt);
      if (obj != null)
        return (short) obj;
      return 0;
    }

    public override int GetInt32(int ordinal)
    {
      object obj = this.queryResult.GetValue(ordinal, VistaDBType.Int);
      if (obj != null)
        return (int) obj;
      return 0;
    }

    public override long GetInt64(int ordinal)
    {
      object obj = this.queryResult.GetValue(ordinal, VistaDBType.BigInt);
      if (obj != null)
        return (long) obj;
      return 0;
    }

    public override string GetName(int ordinal)
    {
      return this.queryResultSchema.GetAliasName(ordinal);
    }

    public override int GetOrdinal(string name)
    {
      return this.queryResultSchema.GetColumnOrdinal(name);
    }

    public override DataTable GetSchemaTable()
    {
      return this.queryResultSchema.GetSchemaTable();
    }

    public override string GetString(int ordinal)
    {
      return (string) this.queryResult.GetValue(ordinal, VistaDBType.NChar);
    }

    public override object GetValue(int ordinal)
    {
      return this.queryResult.GetValue(ordinal, VistaDBType.Unknown) ?? Convert.DBNull;
    }

    public override int GetValues(object[] values)
    {
      int num = values.Length < this.FieldCount ? values.Length : this.FieldCount;
      for (int index = 0; index < num; ++index)
      {
        object obj = this.queryResult.GetValue(index, VistaDBType.Unknown);
        values[index] = obj == null ? Convert.DBNull : obj;
      }
      return num;
    }

    public override bool IsDBNull(int ordinal)
    {
      return this.queryResult.IsNull(ordinal);
    }

    private bool GoNextResult()
    {
      if (this.queryResult != null)
        this.queryResult.Close();
      this.queryResult = (IQueryResult) null;
      this.queryResultSchema = (IQuerySchemaInfo) null;
      this.affectedRows = -1L;
      if (this.pipedReader == null)
      {
        if (this.pipe.Count == 0)
        {
          for (INextQueryResult nextQueryResult = this.queryStatements.NextResult(this.pipe); nextQueryResult != null; nextQueryResult = this.queryStatements.NextResult(this.pipe))
          {
            if (nextQueryResult.AffectedRows > 0L)
            {
              if (this.affectedRows < 0L)
                ++this.affectedRows;
              this.affectedRows += nextQueryResult.AffectedRows;
            }
            if (this.pipe.Count > 0 && this.GoNextResult())
              return true;
            if (nextQueryResult.ResultSet != null)
            {
              this.queryResultSchema = nextQueryResult.Schema;
              if (this.schemaOnly)
              {
                if (nextQueryResult.ResultSet != null)
                {
                  nextQueryResult.ResultSet.Close();
                  break;
                }
                break;
              }
              this.queryResult = nextQueryResult.ResultSet;
              break;
            }
          }
          this.first = this.queryResult != null;
          if (this.queryResult == null)
            return this.queryResultSchema != null;
          return true;
        }
        this.pipedReader = this.pipe.DequeueReader();
      }
      else if (!this.pipedReader.GoNextResult())
      {
        this.pipedReader.Close();
        this.pipedReader = (VistaDBDataReader) null;
        return this.GoNextResult();
      }
      this.affectedRows = this.pipedReader.affectedRows;
      this.queryResult = this.pipedReader.queryResult;
      this.queryResultSchema = this.pipedReader.queryResultSchema;
      this.first = this.queryResult != null;
      if (this.queryResult == null)
        return this.queryResultSchema != null;
      return true;
    }

    public override bool NextResult()
    {
      if (this.queryResult == null && this.queryResultSchema == null)
        return false;
      return this.GoNextResult();
    }

    public override bool Read()
    {
      if (this.queryResult == null)
        return false;
      if (this.first)
      {
        this.first = false;
        return !this.queryResult.EndOfTable;
      }
      this.queryResult.NextRow();
      return !this.queryResult.EndOfTable;
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
      object scalar1 = (object) null;
      if (this.pipedReader == null)
      {
        if (this.pipe.Count == 0)
        {
          object scalar2 = (object) null;
          for (INextQueryResult nextQueryResult = this.queryStatements.NextResult(this.pipe); nextQueryResult != null; nextQueryResult = this.queryStatements.NextResult(this.pipe))
          {
            affectedRows += nextQueryResult.AffectedRows;
            if (this.pipe.Count > 0)
            {
              object obj = this.ExecQuery(ref affectedRows);
              if (scalar2 == null)
                scalar2 = obj;
            }
            this.EvaluateScalar(nextQueryResult.ResultSet, ref scalar2);
          }
          return scalar2;
        }
        this.pipedReader = this.pipe.DequeueReader();
        this.EvaluateScalar(this.pipedReader.queryResult, ref scalar1);
      }
      else
      {
        scalar1 = this.pipedReader.ExecQuery(ref affectedRows);
        this.pipedReader.Close();
        this.pipedReader = (VistaDBDataReader) null;
      }
      return scalar1;
    }

    internal INextQueryResult CurrentResult
    {
      get
      {
        return (INextQueryResult) new BatchStatement.ResultSetData(this.queryResult, this.queryResultSchema, this.affectedRows);
      }
    }

    private void LockQuery()
    {
      if (this.queryStatements == null)
        return;
      this.queryStatements.LockedDisposing = true;
    }

    private void UnlockQuery()
    {
      if (this.queryStatements == null)
        return;
      this.queryStatements.LockedDisposing = false;
    }
  }
}
