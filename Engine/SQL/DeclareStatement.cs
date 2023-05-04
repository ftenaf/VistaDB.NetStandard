using System.Collections.Generic;
using System.Data;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal class DeclareStatement : Statement
  {
    private List<SQLParser.VariableDeclaration> variables;

    public DeclareStatement(LocalSQLConnection connection, Statement parent, SQLParser parser, long id)
      : base(connection, parent, parser, id)
    {
    }

    protected override void OnParse(LocalSQLConnection connection, SQLParser parser)
    {
      parser.SkipToken(true);
      variables = parser.ParseVariables();
      if (variables == null)
        throw new VistaDBSQLException(509, "Missing @variable. Example: DECLARE @variable Integer;", lineNo, symbolNo);
      foreach (SQLParser.VariableDeclaration variable in variables)
      {
        if (DoGetParam(variable.Name) != null)
          throw new VistaDBSQLException(620, 64.ToString() + variable.Name, lineNo, symbolNo);
        DoSetParam(variable.Name, null, variable.DataType, ParameterDirection.Input);
      }
    }

    protected override VistaDBType OnPrepareQuery()
    {
      foreach (SQLParser.VariableDeclaration variable in variables)
      {
        if (variable.Signature != null)
        {
          int num = (int) variable.Signature.Prepare();
        }
        if (variable.DataType == VistaDBType.Unknown && parent.DoGetTemporaryTableName(variable.Name) != null)
          parent.DoGetTemporaryTableName(variable.Name).ExecuteQuery();
      }
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      if (variables == null)
        return null;
      foreach (SQLParser.VariableDeclaration variable in variables)
      {
        Signature signature = variable.Signature;
        if (signature != null)
        {
          if (variable.Default == null)
            variable.Default = Database.CreateEmptyColumn(variable.DataType);
          Database.Conversion.Convert(signature.Execute(), variable.Default);
          signature.SetChanged();
          DoSetParam(variable.Name, variable.Default.Value, variable.DataType, ParameterDirection.Input);
        }
        else
          DoSetParam(variable.Name, null, variable.DataType, ParameterDirection.Input);
      }
      return null;
    }
  }
}
