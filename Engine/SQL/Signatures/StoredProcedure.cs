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
      this.skipNull = false;
    }

    protected override void ParseParameters(SQLParser parser)
    {
      parser.SkipToken(false);
      base.ParseParameters(parser);
    }

    public override SignatureType OnPrepare()
    {
      this.PrepareQueryStatement();
      int num = (int) base.OnPrepare();
      return SignatureType.Expression;
    }

    internal override void DisposeSubProgramStatement()
    {
      if (this.bodyStatement == null)
        return;
      this.bodyStatement.Dispose();
    }

    private void PrepareQueryStatement()
    {
      if (this.bodyStatement != null)
        this.parameters.Clear();
      this.bodyStatement = (Statement) this.parent.Connection.CreateStoredProcedureStatement(this.parent, this.sp.Statement, out this.variables);
      if (this.variables == null)
        return;
      this.parameters = this.CheckParameters();
      this.parameterTypes = new VistaDBType[this.variables.Count];
      this.paramValues = new IColumn[this.variables.Count];
      int paramPos = 0;
      foreach (SQLParser.VariableDeclaration variable in this.variables)
      {
        if (this.bodyStatement.DoGetParam(variable.Name) != null)
          throw new VistaDBSQLException(620, 64.ToString() + variable.Name, this.lineNo, this.symbolNo);
        if (this.IsOutParameter(paramPos, variable.Name))
        {
          if (variable.Direction == ParameterDirection.Input)
            throw new VistaDBSQLException(639, variable.Name, this.LineNo, this.symbolNo);
          this.bodyStatement.DoSetParam(variable.Name, this.ResolveParameter(variable.Name, paramPos));
        }
        else
          this.bodyStatement.DoSetParam(variable.Name, (object) null, variable.DataType, ParameterDirection.Input);
        this.parameterTypes[paramPos++] = variable.DataType;
      }
    }

    private IParameter ResolveParameter(string paramName, int paramPos)
    {
      string paramName1 = paramName;
      if (this.namedParams.ContainsKey(paramName))
      {
        paramName1 = this.namedParams[paramName].Text.Substring(1);
      }
      else
      {
        Signature parameter = this.parameters[paramPos];
        if (parameter != (Signature) null && parameter.SignatureType == SignatureType.Parameter)
          paramName1 = parameter.Text.Substring(1);
      }
      return this.parent.DoGetParam(paramName1);
    }

    private bool IsOutParameter(int paramPos, string paramName)
    {
      if (this.parameters[paramPos] != (Signature) null)
      {
        if (paramPos < this.outParams.Count)
          return this.outParams[paramPos];
        return false;
      }
      IParameter parameter = this.parent.DoGetParam(paramName);
      if (parameter == null)
        return false;
      if (parameter.Direction != ParameterDirection.Output)
        return parameter.Direction == ParameterDirection.InputOutput;
      return true;
    }

    private List<Signature> CheckParameters()
    {
      if (this.parameters.Count + this.namedParams.Count > this.variables.Count)
        throw new VistaDBSQLException(640, this.Text, this.lineNo, this.symbolNo);
      List<Signature> signatureList = new List<Signature>(this.variables.Count);
      int num = 0;
      foreach (SQLParser.VariableDeclaration variable in this.variables)
      {
        if (this.namedParams.ContainsKey(variable.Name))
        {
          signatureList.Add(this.namedParams[variable.Name]);
        }
        else
        {
          Signature signature = (Signature) null;
          if (num < this.parameters.Count)
            signature = this.parameters[num++];
          if (signature == (Signature) null)
          {
            if (this.parent.DoGetParam(variable.Name) != null)
            {
              signatureList.Add((Signature) null);
            }
            else
            {
              if (variable.Default == null)
                throw new VistaDBSQLException(641, variable.Name, this.LineNo, this.SymbolNo);
              IColumn column = this.CreateColumn(variable.DataType);
              this.parent.Database.Conversion.Convert(variable.Default, (IValue) column);
              signatureList.Add((Signature) ConstantSignature.CreateSignature(column, this.parent));
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
      if (this.variables != null)
      {
        int index = 0;
        foreach (SQLParser.VariableDeclaration variable in this.variables)
        {
          if (index < this.parameters.Count && this.parameters[index] != (Signature) null)
            this.bodyStatement.DoSetParam(variable.Name, ((IValue) this.paramValues[index]).Value, variable.DataType, variable.Direction);
          else
            this.bodyStatement.DoSetParam(variable.Name, this.parent.DoGetParam(variable.Name).Value, variable.DataType, variable.Direction);
          ++index;
        }
      }
      this.bodyStatement.DoSetReturnParameter(this.returnParameter);
      IQueryResult queryResult = this.bodyStatement.ExecuteQuery();
      if (this.bodyStatement.AffectedRows > 0L)
        this.parent.AffectedRows += this.bodyStatement.AffectedRows;
      return (object) queryResult;
    }
  }
}
