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
      dataType = VistaDBType.Unknown;
      if (ParamCount < 1 || ParamCount > 2)
        throw new VistaDBSQLException(501, "LASTIDENTITY", lineNo, symbolNo);
      if (ParamCount == 2)
      {
        if (this[0].SignatureType != SignatureType.Column || this[1].SignatureType != SignatureType.Column)
          throw new VistaDBSQLException(550, "LASTIDENTITY", lineNo, symbolNo);
        ColumnSignature columnSignature1 = (ColumnSignature) this[0];
        ColumnSignature columnSignature2 = (ColumnSignature) this[1];
        if (columnSignature1.TableAlias != null || columnSignature2.TableAlias != null)
          throw new VistaDBSQLException(550, "LASTIDENTITY", lineNo, symbolNo);
        columnName = columnSignature1.ColumnName;
        tableName = columnSignature2.ColumnName;
        parameters.Clear();
        parameterTypes = (VistaDBType[]) null;
      }
      else
      {
        parameterTypes[0] = VistaDBType.Unknown;
        columnName = (string) null;
        tableName = (string) null;
      }
      table = (SourceTable) null;
    }

    public override SignatureType OnPrepare()
    {
      int num = (int) base.OnPrepare();
      if (ParamCount == 1)
      {
        ColumnSignature columnSignature = (ColumnSignature) this[0];
        dataType = columnSignature.DataType;
        table = columnSignature.Table;
        columnName = columnSignature.ColumnName;
        parameters.Clear();
        parameterTypes = (VistaDBType[]) null;
      }
      else
        dataType = parent.Database.TableSchema(tableName)[columnName].Type;
      return SignatureType.Expression;
    }

    protected override object ExecuteSubProgram()
    {
      if (ParamCount == 1)
        return ((IValue) table.GetLastIdentity(columnName))?.Value;
      return parent.Database.GetLastIdentity(tableName, columnName)?.Value;
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }
  }
}
