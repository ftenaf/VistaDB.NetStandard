using System;
using System.Globalization;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class RenameFunction : Function
  {
    private const string COLUMN_TYPE = "COLUMN";
    private const string TABLE_TYPE = "OBJECT";
    private const string VIEW_TYPE = "VIEW";

    public RenameFunction(SQLParser parser)
      : base(parser, -1, true)
    {
      if (this.ParamCount < 2 || this.ParamCount > 3)
        throw new VistaDBSQLException(501, "SP_RENAME", this.lineNo, this.symbolNo);
      this.dataType = VistaDBType.Bit;
      this.parameterTypes[0] = VistaDBType.NChar;
      this.parameterTypes[1] = VistaDBType.NChar;
      if (this.ParamCount == 3)
        this.parameterTypes[2] = VistaDBType.NChar;
      this.parent.SetHasDDL();
    }

    protected override bool AllowProcedureSyntax()
    {
      return true;
    }

    protected override bool AllowFunctionSyntax()
    {
      return true;
    }

    protected override object ExecuteSubProgram()
    {
      string columnName = (string) ((IValue) this.paramValues[0]).Value;
      string name = (string) ((IValue) this.paramValues[1]).Value;
      int startIndex = 0;
      string namePart = SQLParser.GetNamePart(ref startIndex, name, this.lineNo, this.symbolNo);
      if (startIndex != -1)
        throw new VistaDBSQLException(628, (string) ((IValue) this.paramValues[1]).Value, this.lineNo, this.symbolNo);
      IVistaDBTableSchema schema = (IVistaDBTableSchema) null;
      string hint = this.ParamCount == 2 ? "OBJECT" : (string) ((IValue) this.paramValues[2]).Value;
      bool flag = true;
      try
      {
        string str;
        switch (hint.ToUpper(CultureInfo.InvariantCulture))
        {
          case "OBJECT":
            str = SQLParser.GetTableName(columnName, TokenType.ComplexName, this.lineNo, this.symbolNo);
            schema = this.parent.Database.TableSchema(str);
            schema.Name = namePart;
            break;
          case "COLUMN":
            str = SQLParser.GetColumnAndTableName(columnName, TokenType.ComplexName, this.lineNo, this.symbolNo, out columnName, false);
            schema = this.parent.Database.TableSchema(str);
            schema.AlterColumnName(columnName, namePart);
            break;
          case "VIEW":
            string tableName = SQLParser.GetTableName(columnName, TokenType.ComplexName, this.lineNo, this.symbolNo);
            Database.ViewList viewList = (Database.ViewList) this.parent.Database.EnumViews();
            Database.ViewList.View view = (Database.ViewList.View) viewList[(object) tableName];
            if (view == null)
              throw new VistaDBSQLException(606, tableName, this.lineNo, this.symbolNo);
            if (viewList.ContainsKey((object) namePart))
              throw new VistaDBSQLException(603, namePart, this.lineNo, this.symbolNo);
            try
            {
              this.parent.Database.DeleteViewObject((IView) view);
              view.Name = namePart;
              this.parent.Database.CreateViewObject((IView) view);
              return (object) true;
            }
            catch
            {
              if (!this.parent.Database.EnumViews().Contains((object) tableName))
              {
                view.Name = tableName;
                this.parent.Database.CreateViewObject((IView) view);
              }
              throw;
            }
          default:
            throw new VistaDBSQLException(629, hint, this.lineNo, this.symbolNo);
        }
        try
        {
          this.parent.Database.AlterTable(str, schema);
        }
        catch (Exception ex)
        {
          flag = false;
        }
      }
      finally
      {
        schema?.Dispose();
      }
      return (object) flag;
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }
  }
}
