using System.Collections.Generic;
using System.Data;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class StoredProcedure : Procedure
  {
    private IStoredProcedureInformation sp;
    private Statement bodyStatement;

    internal StoredProcedure(SQLParser parser, IStoredProcedureInformation sp)
      : base(parser, -1, false)
    {
      this.sp = sp;
      skipNull = false;
    }

    protected override void ParseParameters(SQLParser parser)
    {
      parser.SkipToken(false);
      base.ParseParameters(parser);
    }

    public override SignatureType OnPrepare()
    {
      PrepareQueryStatement();
      int num = (int) base.OnPrepare();
      return SignatureType.Expression;
    }

    internal override void DisposeSubProgramStatement()
    {
      if (bodyStatement == null)
        return;
      bodyStatement.Dispose();
    }

    private void PrepareQueryStatement()
    {
      if (bodyStatement != null)
        parameters.Clear();
      bodyStatement = (Statement) parent.Connection.CreateStoredProcedureStatement(parent, sp.Statement, out variables);
      if (variables == null)
        return;
      parameters = CheckParameters();
      parameterTypes = new VistaDBType[variables.Count];
      paramValues = new IColumn[variables.Count];
      int paramPos = 0;
      foreach (SQLParser.VariableDeclaration variable in variables)
      {
        if (bodyStatement.DoGetParam(variable.Name) != null)
          throw new VistaDBSQLException(620, 64.ToString() + variable.Name, lineNo, symbolNo);
        if (IsOutParameter(paramPos, variable.Name))
        {
          if (variable.Direction == ParameterDirection.Input)
            throw new VistaDBSQLException(639, variable.Name, LineNo, symbolNo);
          bodyStatement.DoSetParam(variable.Name, ResolveParameter(variable.Name, paramPos));
        }
        else
          bodyStatement.DoSetParam(variable.Name, (object) null, variable.DataType, ParameterDirection.Input);
        parameterTypes[paramPos++] = variable.DataType;
      }
    }

    private IParameter ResolveParameter(string paramName, int paramPos)
    {
      string paramName1 = paramName;
      if (namedParams.ContainsKey(paramName))
      {
        paramName1 = namedParams[paramName].Text.Substring(1);
      }
      else
      {
        Signature parameter = parameters[paramPos];
        if (parameter != (Signature) null && parameter.SignatureType == SignatureType.Parameter)
          paramName1 = parameter.Text.Substring(1);
      }
      return parent.DoGetParam(paramName1);
    }

    private bool IsOutParameter(int paramPos, string paramName)
    {
      if (parameters[paramPos] != (Signature) null)
      {
        if (paramPos < outParams.Count)
          return outParams[paramPos];
        return false;
      }
      IParameter parameter = parent.DoGetParam(paramName);
      if (parameter == null)
        return false;
      if (parameter.Direction != ParameterDirection.Output)
        return parameter.Direction == ParameterDirection.InputOutput;
      return true;
    }

    private List<Signature> CheckParameters()
    {
      if (parameters.Count + namedParams.Count > variables.Count)
        throw new VistaDBSQLException(640, Text, lineNo, symbolNo);
      List<Signature> signatureList = new List<Signature>(variables.Count);
      int num = 0;
      foreach (SQLParser.VariableDeclaration variable in variables)
      {
        if (namedParams.ContainsKey(variable.Name))
        {
          signatureList.Add(namedParams[variable.Name]);
        }
        else
        {
          Signature signature = (Signature) null;
          if (num < parameters.Count)
            signature = parameters[num++];
          if (signature == (Signature) null)
          {
            if (parent.DoGetParam(variable.Name) != null)
            {
              signatureList.Add((Signature) null);
            }
            else
            {
              if (variable.Default == null)
                throw new VistaDBSQLException(641, variable.Name, LineNo, SymbolNo);
              IColumn column = CreateColumn(variable.DataType);
              parent.Database.Conversion.Convert(variable.Default, (IValue) column);
              signatureList.Add((Signature) ConstantSignature.CreateSignature(column, parent));
            }
          }
          else
            signatureList.Add(signature);
        }
      }
      return signatureList;
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }

    protected override object ExecuteSubProgram()
    {
      if (variables != null)
      {
        int index = 0;
        foreach (SQLParser.VariableDeclaration variable in variables)
        {
          if (index < parameters.Count && parameters[index] != (Signature) null)
            bodyStatement.DoSetParam(variable.Name, ((IValue) paramValues[index]).Value, variable.DataType, variable.Direction);
          else
            bodyStatement.DoSetParam(variable.Name, parent.DoGetParam(variable.Name).Value, variable.DataType, variable.Direction);
          ++index;
        }
      }
      bodyStatement.DoSetReturnParameter(returnParameter);
      IQueryResult queryResult = bodyStatement.ExecuteQuery();
      if (bodyStatement.AffectedRows > 0L)
        parent.AffectedRows += bodyStatement.AffectedRows;
      return (object) queryResult;
    }
  }
}
