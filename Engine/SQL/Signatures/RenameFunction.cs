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
        public RenameFunction(SQLParser parser)
      : base(parser, -1, true)
    {
      if (ParamCount < 2 || ParamCount > 3)
        throw new VistaDBSQLException(501, "SP_RENAME", lineNo, symbolNo);
      dataType = VistaDBType.Bit;
      parameterTypes[0] = VistaDBType.NChar;
      parameterTypes[1] = VistaDBType.NChar;
      if (ParamCount == 3)
        parameterTypes[2] = VistaDBType.NChar;
      parent.SetHasDDL();
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
      string columnName = (string) ((IValue) paramValues[0]).Value;
      string name = (string) ((IValue) paramValues[1]).Value;
      int startIndex = 0;
      string namePart = SQLParser.GetNamePart(ref startIndex, name, lineNo, symbolNo);
      if (startIndex != -1)
        throw new VistaDBSQLException(628, (string) ((IValue) paramValues[1]).Value, lineNo, symbolNo);
      IVistaDBTableSchema schema = (IVistaDBTableSchema) null;
      string hint = ParamCount == 2 ? "OBJECT" : (string) ((IValue) paramValues[2]).Value;
      bool flag = true;
      try
      {
        string str;
        switch (hint.ToUpper(CultureInfo.InvariantCulture))
        {
          case "OBJECT":
            str = SQLParser.GetTableName(columnName, TokenType.ComplexName, lineNo, symbolNo);
            schema = parent.Database.TableSchema(str);
            schema.Name = namePart;
            break;
          case "COLUMN":
            str = SQLParser.GetColumnAndTableName(columnName, TokenType.ComplexName, lineNo, symbolNo, out columnName, false);
            schema = parent.Database.TableSchema(str);
            schema.AlterColumnName(columnName, namePart);
            break;
          case "VIEW":
            string tableName = SQLParser.GetTableName(columnName, TokenType.ComplexName, lineNo, symbolNo);
            Database.ViewList viewList = (Database.ViewList) parent.Database.EnumViews();
            Database.ViewList.View view = (Database.ViewList.View) viewList[(object) tableName];
            if (view == null)
              throw new VistaDBSQLException(606, tableName, lineNo, symbolNo);
            if (viewList.ContainsKey((object) namePart))
              throw new VistaDBSQLException(603, namePart, lineNo, symbolNo);
            try
            {
              parent.Database.DeleteViewObject((IView) view);
              view.Name = namePart;
              parent.Database.CreateViewObject((IView) view);
              return (object) true;
            }
            catch
            {
              if (!parent.Database.EnumViews().Contains((object) tableName))
              {
                view.Name = tableName;
                parent.Database.CreateViewObject((IView) view);
              }
              throw;
            }
          default:
            throw new VistaDBSQLException(629, hint, lineNo, symbolNo);
        }
        try
        {
          parent.Database.AlterTable(str, schema);
        }
        catch (Exception)
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
