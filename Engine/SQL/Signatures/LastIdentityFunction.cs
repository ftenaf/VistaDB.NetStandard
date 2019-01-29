using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LastIdentityFunction : Function
  {
    private SourceTable table;
    private string columnName;
    private string tableName;

    public LastIdentityFunction(SQLParser parser)
      : base(parser, -1, true)
    {
      this.dataType = VistaDBType.Unknown;
      if (this.ParamCount < 1 || this.ParamCount > 2)
        throw new VistaDBSQLException(501, "LASTIDENTITY", this.lineNo, this.symbolNo);
      if (this.ParamCount == 2)
      {
        if (this[0].SignatureType != SignatureType.Column || this[1].SignatureType != SignatureType.Column)
          throw new VistaDBSQLException(550, "LASTIDENTITY", this.lineNo, this.symbolNo);
        ColumnSignature columnSignature1 = (ColumnSignature) this[0];
        ColumnSignature columnSignature2 = (ColumnSignature) this[1];
        if (columnSignature1.TableAlias != null || columnSignature2.TableAlias != null)
          throw new VistaDBSQLException(550, "LASTIDENTITY", this.lineNo, this.symbolNo);
        this.columnName = columnSignature1.ColumnName;
        this.tableName = columnSignature2.ColumnName;
        this.parameters.Clear();
        this.parameterTypes = (VistaDBType[]) null;
      }
      else
      {
        this.parameterTypes[0] = VistaDBType.Unknown;
        this.columnName = (string) null;
        this.tableName = (string) null;
      }
      this.table = (SourceTable) null;
    }

    public override SignatureType OnPrepare()
    {
      int num = (int) base.OnPrepare();
      if (this.ParamCount == 1)
      {
        ColumnSignature columnSignature = (ColumnSignature) this[0];
        this.dataType = columnSignature.DataType;
        this.table = columnSignature.Table;
        this.columnName = columnSignature.ColumnName;
        this.parameters.Clear();
        this.parameterTypes = (VistaDBType[]) null;
      }
      else
        this.dataType = this.parent.Database.TableSchema(this.tableName)[this.columnName].Type;
      return SignatureType.Expression;
    }

    protected override object ExecuteSubProgram()
    {
      if (this.ParamCount == 1)
        return ((IValue) this.table.GetLastIdentity(this.columnName))?.Value;
      return this.parent.Database.GetLastIdentity(this.tableName, this.columnName)?.Value;
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }
  }
}
