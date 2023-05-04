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
      storedFunction = udf;
      skipNull = false;
    }

    protected override void ParseParameters(SQLParser parser)
    {
      if (!parser.IsToken("("))
        parser.SkipToken(true);
      base.ParseParameters(parser);
    }

    public override SignatureType OnPrepare()
    {
      PrepareQueryStatement();
      if (paramValues.Length != parameters.Count)
        throw new VistaDBSQLException(501, text, lineNo, symbolNo);
      int num = (int) base.OnPrepare();
      return SignatureType.Expression;
    }

    protected override object ExecuteSubProgram()
    {
      if (variables != null)
      {
        uint num = 0;
        foreach (SQLParser.VariableDeclaration variable in variables)
        {
          bodyStatement.DoSetParam(variable.Name, ((IValue) paramValues[num]).Value, variable.DataType, variable.Direction);
          ++num;
        }
      }
      bodyStatement.DoSetReturnParameter(returnParameter);
      bodyStatement.ExecuteQuery();
      return returnParameter.Value;
    }

    private void PrepareQueryStatement()
    {
      dataType = VistaDBType.Int;
      if (bodyStatement != null)
        parameters.Clear();
      bodyStatement = parent.Connection.CreateStoredFunctionStatement(parent, storedFunction.Statement, out dataType, out variables, out resultTableStatement);
      returnParameter = (IParameter) new BatchStatement.ParamInfo((object) null, dataType, ParameterDirection.ReturnValue);
      if (resultTableStatement != null)
        resultTableStatement.ExecuteQuery();
      if (variables == null)
        return;
      ThreadDefaultValues();
      parameterTypes = new VistaDBType[variables.Count];
      paramValues = new IColumn[variables.Count];
      int num = 0;
      foreach (SQLParser.VariableDeclaration variable in variables)
      {
        if (bodyStatement.DoGetParam(variable.Name) != null)
          throw new VistaDBSQLException(620, 64.ToString() + variable.Name, lineNo, symbolNo);
        bodyStatement.DoSetParam(variable.Name, (object) null, variable.DataType, ParameterDirection.Input);
        parameterTypes[num++] = variable.DataType;
      }
    }

    private void ThreadDefaultValues()
    {
      int index = -1;
      foreach (SQLParser.VariableDeclaration variable in variables)
      {
        ++index;
        if (!(parameters[index] != (Signature) null))
        {
          if (variable.Default == null)
            throw new VistaDBSQLException(641, variable.Name, LineNo, SymbolNo);
          IColumn column = CreateColumn(variable.DataType);
          parent.Database.Conversion.Convert(variable.Default, (IValue) column);
          parameters[index] = (Signature) ConstantSignature.CreateSignature(column, parent);
        }
      }
    }
  }
}
