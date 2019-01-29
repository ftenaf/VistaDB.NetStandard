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
      this.variables = parser.ParseVariables();
      if (this.variables == null)
        throw new VistaDBSQLException(509, "Missing @variable. Example: DECLARE @variable Integer;", this.lineNo, this.symbolNo);
      foreach (SQLParser.VariableDeclaration variable in this.variables)
      {
        if (this.DoGetParam(variable.Name) != null)
          throw new VistaDBSQLException(620, 64.ToString() + variable.Name, this.lineNo, this.symbolNo);
        this.DoSetParam(variable.Name, (object) null, variable.DataType, ParameterDirection.Input);
      }
    }

    protected override VistaDBType OnPrepareQuery()
    {
      foreach (SQLParser.VariableDeclaration variable in this.variables)
      {
        if (variable.Signature != (Signature) null)
        {
          int num = (int) variable.Signature.Prepare();
        }
        if (variable.DataType == VistaDBType.Unknown && this.parent.DoGetTemporaryTableName(variable.Name) != null)
          this.parent.DoGetTemporaryTableName(variable.Name).ExecuteQuery();
      }
      return VistaDBType.Unknown;
    }

    protected override IQueryResult OnExecuteQuery()
    {
      if (this.variables == null)
        return (IQueryResult) null;
      foreach (SQLParser.VariableDeclaration variable in this.variables)
      {
        Signature signature = variable.Signature;
        if (signature != (Signature) null)
        {
          if (variable.Default == null)
            variable.Default = (IValue) this.Database.CreateEmptyColumn(variable.DataType);
          this.Database.Conversion.Convert((IValue) signature.Execute(), variable.Default);
          signature.SetChanged();
          this.DoSetParam(variable.Name, variable.Default.Value, variable.DataType, ParameterDirection.Input);
        }
        else
          this.DoSetParam(variable.Name, (object) null, variable.DataType, ParameterDirection.Input);
      }
      return (IQueryResult) null;
    }
  }
}
