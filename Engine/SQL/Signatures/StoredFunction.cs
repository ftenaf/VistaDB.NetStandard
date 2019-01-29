using System;
using System.Collections.Generic;
using System.Data;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class StoredFunction : Function
  {
    protected IUserDefinedFunctionInformation storedFunction;
    protected StoredFunctionBody bodyStatement;
    protected List<SQLParser.VariableDeclaration> variables;
    protected CreateTableStatement resultTableStatement;

    internal StoredFunction(SQLParser parser, IUserDefinedFunctionInformation udf)
      : base(parser, -1, false)
    {
      this.storedFunction = udf;
      this.skipNull = false;
    }

    protected override void ParseParameters(SQLParser parser)
    {
      if (!parser.IsToken("("))
        parser.SkipToken(true);
      base.ParseParameters(parser);
    }

    public override SignatureType OnPrepare()
    {
      this.PrepareQueryStatement();
      if (this.paramValues.Length != this.parameters.Count)
        throw new VistaDBSQLException(501, this.text, this.lineNo, this.symbolNo);
      int num = (int) base.OnPrepare();
      return SignatureType.Expression;
    }

    protected override object ExecuteSubProgram()
    {
      if (this.variables != null)
      {
        uint num = 0;
        foreach (SQLParser.VariableDeclaration variable in this.variables)
        {
          this.bodyStatement.DoSetParam(variable.Name, ((IValue) this.paramValues[num]).Value, variable.DataType, variable.Direction);
          ++num;
        }
      }
      this.bodyStatement.DoSetReturnParameter(this.returnParameter);
      this.bodyStatement.ExecuteQuery();
      return this.returnParameter.Value;
    }

    private void PrepareQueryStatement()
    {
      this.dataType = VistaDBType.Int;
      if (this.bodyStatement != null)
        this.parameters.Clear();
      this.bodyStatement = this.parent.Connection.CreateStoredFunctionStatement(this.parent, this.storedFunction.Statement, out this.dataType, out this.variables, out this.resultTableStatement);
      this.returnParameter = (IParameter) new BatchStatement.ParamInfo((object) null, this.dataType, ParameterDirection.ReturnValue);
      if (this.resultTableStatement != null)
        this.resultTableStatement.ExecuteQuery();
      if (this.variables == null)
        return;
      this.ThreadDefaultValues();
      this.parameterTypes = new VistaDBType[this.variables.Count];
      this.paramValues = new IColumn[this.variables.Count];
      int num = 0;
      foreach (SQLParser.VariableDeclaration variable in this.variables)
      {
        if (this.bodyStatement.DoGetParam(variable.Name) != null)
          throw new VistaDBSQLException(620, 64.ToString() + variable.Name, this.lineNo, this.symbolNo);
        this.bodyStatement.DoSetParam(variable.Name, (object) null, variable.DataType, ParameterDirection.Input);
        this.parameterTypes[num++] = variable.DataType;
      }
    }

    private void ThreadDefaultValues()
    {
      int index = -1;
      foreach (SQLParser.VariableDeclaration variable in this.variables)
      {
        ++index;
        if (!(this.parameters[index] != (Signature) null))
        {
          if (variable.Default == null)
            throw new VistaDBSQLException(641, variable.Name, this.LineNo, this.SymbolNo);
          IColumn column = this.CreateColumn(variable.DataType);
          this.parent.Database.Conversion.Convert(variable.Default, (IValue) column);
          this.parameters[index] = (Signature) ConstantSignature.CreateSignature(column, this.parent);
        }
      }
    }
  }
}
