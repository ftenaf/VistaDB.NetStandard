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
      this.skipNull = false;
      this.dataType = VistaDBType.Unknown;
      this.parameterTypes[0] = VistaDBType.VarChar;
      this.parameterTypes[1] = VistaDBType.VarChar;
      this.parameterTypes[2] = VistaDBType.VarChar;
      this.parameterTypes[3] = VistaDBType.Unknown;
    }

    public override SignatureType OnPrepare()
    {
      this.tableName = this.parameters[0].Text;
      this.indexColumnName = this.parameters[1].Text;
      this.resultColumnName = this.parameters[2].Text;
      this[3] = ConstantSignature.PrepareAndCheckConstant(this[3], this.parameterTypes[3]);
      try
      {
        this.db = VistaDBContext.DDAChannel.CurrentDatabase;
        if (this.db == null)
          throw new ArgumentException("Could not open database in Lookup function");
      }
      catch (ArgumentException ex)
      {
        throw;
      }
      catch (Exception ex)
      {
        throw new ArgumentException("Could not open database in Lookup function", ex);
      }
      try
      {
        this.table = this.db.OpenTable(this.tableName, false, true);
        if (this.table == null)
          throw new ArgumentException("Could not open table " + this.tableName + " in Lookup function");
      }
      catch (ArgumentException ex)
      {
        throw;
      }
      catch (Exception ex)
      {
        if (this.table != null)
          this.table.Close();
        throw new ArgumentException("Could not open table " + this.tableName + " in Lookup function", ex);
      }
      try
      {
        this.table.First();
        IVistaDBValue vistaDbValue = this.table.Get(this.indexColumnName);
        if (vistaDbValue == null)
        {
          this.table.Close();
          throw new ArgumentException("Could not find index column " + this.indexColumnName + " in Lookup function");
        }
        this.indexType = vistaDbValue.Type;
      }
      catch (ArgumentException ex)
      {
        throw;
      }
      catch (Exception ex)
      {
        if (this.table != null)
          this.table.Close();
        throw new ArgumentException("Could not find index column " + this.indexColumnName + " in Lookup function", ex);
      }
      VistaDBType type;
      try
      {
        IVistaDBValue vistaDbValue = this.table.Get(this.resultColumnName);
        if (vistaDbValue == null)
          throw new ArgumentException("Could not find result column " + this.resultColumnName + " in Lookup function");
        type = vistaDbValue.Type;
      }
      catch (ArgumentException ex)
      {
        throw;
      }
      catch (Exception ex)
      {
        throw new ArgumentException("Could not find result column " + this.resultColumnName + " in Lookup function", ex);
      }
      finally
      {
        if (this.table != null)
          this.table.Close();
      }
      this.dataType = type;
      this.result = this.CreateColumn(this.dataType);
      this.parameterTypes[3] = this.indexType;
      this.paramValues[3] = this.CreateColumn(this.indexType);
      this.cache = CacheFactory.Instance.GetColumnCache(this.db, this.tableName, this.indexColumnName, this.indexType, this.resultColumnName);
      return SignatureType.ExternalColumn;
    }

    protected override IColumn InternalExecute()
    {
      ((IValue) this.result).Value = this.cache.GetValue(((IValue) this[3].Execute()).Value);
      return this.result;
    }

    protected override object ExecuteSubProgram()
    {
      return ((IValue) this.result).Value;
    }
  }
}
