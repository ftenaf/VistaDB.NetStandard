using System;
using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LookupFunction : Function
  {
    private string tableName;
    private string indexColumnName;
    private string resultColumnName;
    private IVistaDBDatabase db;
    private IVistaDBTable table;
    private VistaDBType indexType;
    private ColumnCache cache;

    internal LookupFunction(SQLParser parser)
      : base(parser, 4, true)
    {
      skipNull = false;
      dataType = VistaDBType.Unknown;
      parameterTypes[0] = VistaDBType.VarChar;
      parameterTypes[1] = VistaDBType.VarChar;
      parameterTypes[2] = VistaDBType.VarChar;
      parameterTypes[3] = VistaDBType.Unknown;
    }

    public override SignatureType OnPrepare()
    {
      tableName = parameters[0].Text;
      indexColumnName = parameters[1].Text;
      resultColumnName = parameters[2].Text;
      this[3] = ConstantSignature.PrepareAndCheckConstant(this[3], parameterTypes[3]);
      try
      {
        db = VistaDBContext.DDAChannel.CurrentDatabase;
        if (db == null)
          throw new ArgumentException("Could not open database in Lookup function");
      }
      catch (ArgumentException)
            {
        throw;
      }
      catch (Exception ex)
      {
        throw new ArgumentException("Could not open database in Lookup function", ex);
      }
      try
      {
        table = db.OpenTable(tableName, false, true);
        if (table == null)
          throw new ArgumentException("Could not open table " + tableName + " in Lookup function");
      }
      catch (ArgumentException)
            {
        throw;
      }
      catch (Exception ex)
      {
        if (table != null)
          table.Close();
        throw new ArgumentException("Could not open table " + tableName + " in Lookup function", ex);
      }
      try
      {
        table.First();
        IVistaDBValue vistaDbValue = table.Get(indexColumnName);
        if (vistaDbValue == null)
        {
          table.Close();
          throw new ArgumentException("Could not find index column " + indexColumnName + " in Lookup function");
        }
        indexType = vistaDbValue.Type;
      }
      catch (ArgumentException)
            {
        throw;
      }
      catch (Exception ex)
      {
        if (table != null)
          table.Close();
        throw new ArgumentException("Could not find index column " + indexColumnName + " in Lookup function", ex);
      }
      VistaDBType type;
      try
      {
        IVistaDBValue vistaDbValue = table.Get(resultColumnName);
        if (vistaDbValue == null)
          throw new ArgumentException("Could not find result column " + resultColumnName + " in Lookup function");
        type = vistaDbValue.Type;
      }
      catch (ArgumentException)
            {
        throw;
      }
      catch (Exception ex)
      {
        throw new ArgumentException("Could not find result column " + resultColumnName + " in Lookup function", ex);
      }
      finally
      {
        if (table != null)
          table.Close();
      }
      dataType = type;
      result = CreateColumn(dataType);
      parameterTypes[3] = indexType;
      paramValues[3] = CreateColumn(indexType);
      cache = CacheFactory.Instance.GetColumnCache(db, tableName, indexColumnName, indexType, resultColumnName);
      return SignatureType.ExternalColumn;
    }

    protected override IColumn InternalExecute()
    {
      ((IValue) result).Value = cache.GetValue(((IValue) this[3].Execute()).Value);
      return result;
    }

    protected override object ExecuteSubProgram()
    {
      return ((IValue) result).Value;
    }
  }
}
