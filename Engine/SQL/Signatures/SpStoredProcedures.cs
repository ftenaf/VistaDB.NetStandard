using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpStoredProcedures : SpecialFunction
  {
    protected int currentParamPos;
    protected int resultType;
    protected List<SQLParser.VariableDeclaration> paramsList;
    protected Statement curentProcStatement;

    internal SpStoredProcedures(SQLParser parser, int resultColumnsCount)
      : base(parser, 0, resultColumnsCount)
    {
      if (ParamCount > 0)
        throw new VistaDBSQLException(501, "SP_STORED_PROCEDURES", lineNo, symbolNo);
      currentParamPos = -1;
      resultColumnTypes[0] = VistaDBType.NVarChar;
      resultColumnTypes[1] = VistaDBType.NVarChar;
      resultColumnTypes[2] = VistaDBType.NText;
      resultColumnTypes[3] = VistaDBType.SmallInt;
      resultColumnTypes[4] = VistaDBType.NVarChar;
      resultColumnTypes[5] = VistaDBType.Int;
      resultColumnTypes[6] = VistaDBType.Bit;
      resultColumnTypes[7] = VistaDBType.NVarChar;
      resultColumnNames[0] = "PROC_NAME";
      resultColumnNames[1] = "PROC_DESCRIPTION";
      resultColumnNames[2] = "PROC_BODY";
      resultColumnNames[3] = "PARAM_ORDER";
      resultColumnNames[4] = "PARAM_NAME";
      resultColumnNames[5] = "PARAM_TYPE";
      resultColumnNames[6] = "IS_PARAM_OUT";
      resultColumnNames[7] = "DEFAULT_VALUE";
    }

    internal SpStoredProcedures(SQLParser parser)
      : this(parser, 8)
    {
    }

    protected virtual int ParseProcedure(string procStatement)
    {
      try
      {
        curentProcStatement = parent.Connection.CreateStoredProcedureStatement(parent, procStatement, out paramsList);
      }
      catch (Exception)
            {
      }
      currentParamPos = 0;
      return -1;
    }

    protected virtual void FillRow(IRow row, IStoredProcedureInformation sp)
    {
      if (currentParamPos < 0)
        resultType = ParseProcedure(sp.Statement);
            row[0].Value = sp.Name;
            row[1].Value = sp.Description;
            row[2].Value = sp.Statement;
      if (paramsList == null)
      {
        currentParamPos = -1;
                row[3].Value = (short)-1;
                row[4].Value = string.Empty;
                row[5].Value = 31;
                row[6].Value = false;
      }
      else
      {
        SQLParser.VariableDeclaration variableDeclaration = paramsList[currentParamPos];
                row[3].Value = (short)currentParamPos;
                row[4].Value = 64.ToString() + variableDeclaration.Name;
                row[5].Value = variableDeclaration.DataType;
                row[6].Value = variableDeclaration.Direction != ParameterDirection.Input;
        if (variableDeclaration.Default != null)
        {
          string str = variableDeclaration.Default.ToString();
          if (string.IsNullOrEmpty(str))
                        row[7].Value = 39.ToString() + '\'';
          else if (str.Equals("<null>", StringComparison.OrdinalIgnoreCase))
                        row[7].Value = "NULL";
          else
                        row[7].Value = variableDeclaration.DataType.ToString().EndsWith(VistaDBType.Char.ToString()) ? 39.ToString() + str + '\'' : (object) str;
        }
        else
                    row[7].Value = string.Empty;
        if (++currentParamPos < paramsList.Count)
          return;
        currentParamPos = -1;
      }
    }

    public override bool First(IRow row)
    {
      enumerator.Reset();
      if (!enumerator.MoveNext())
        return false;
      FillRow(row, enumerator.Current as IStoredProcedureInformation);
      return true;
    }

    public override bool GetNextResult(IRow row)
    {
      if (currentParamPos < 0 && !enumerator.MoveNext())
        return false;
      FillRow(row, enumerator.Current as IStoredProcedureInformation);
      return true;
    }

    public override void Close()
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    protected override object ExecuteSubProgram()
    {
      enumerator = Parent.Database.GetStoredProcedures().GetEnumerator();
      return null;
    }
  }
}
