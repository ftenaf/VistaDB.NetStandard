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
      if (this.ParamCount > 0)
        throw new VistaDBSQLException(501, "SP_STORED_PROCEDURES", this.lineNo, this.symbolNo);
      this.currentParamPos = -1;
      this.resultColumnTypes[0] = VistaDBType.NVarChar;
      this.resultColumnTypes[1] = VistaDBType.NVarChar;
      this.resultColumnTypes[2] = VistaDBType.NText;
      this.resultColumnTypes[3] = VistaDBType.SmallInt;
      this.resultColumnTypes[4] = VistaDBType.NVarChar;
      this.resultColumnTypes[5] = VistaDBType.Int;
      this.resultColumnTypes[6] = VistaDBType.Bit;
      this.resultColumnTypes[7] = VistaDBType.NVarChar;
      this.resultColumnNames[0] = "PROC_NAME";
      this.resultColumnNames[1] = "PROC_DESCRIPTION";
      this.resultColumnNames[2] = "PROC_BODY";
      this.resultColumnNames[3] = "PARAM_ORDER";
      this.resultColumnNames[4] = "PARAM_NAME";
      this.resultColumnNames[5] = "PARAM_TYPE";
      this.resultColumnNames[6] = "IS_PARAM_OUT";
      this.resultColumnNames[7] = "DEFAULT_VALUE";
    }

    internal SpStoredProcedures(SQLParser parser)
      : this(parser, 8)
    {
    }

    protected virtual int ParseProcedure(string procStatement)
    {
      try
      {
        this.curentProcStatement = (Statement) this.parent.Connection.CreateStoredProcedureStatement(this.parent, procStatement, out this.paramsList);
      }
      catch (Exception ex)
      {
      }
      this.currentParamPos = 0;
      return -1;
    }

    protected virtual void FillRow(IRow row, IStoredProcedureInformation sp)
    {
      if (this.currentParamPos < 0)
        this.resultType = this.ParseProcedure(sp.Statement);
      ((IValue) row[0]).Value = (object) sp.Name;
      ((IValue) row[1]).Value = (object) sp.Description;
      ((IValue) row[2]).Value = (object) sp.Statement;
      if (this.paramsList == null)
      {
        this.currentParamPos = -1;
        ((IValue) row[3]).Value = (object) (short) -1;
        ((IValue) row[4]).Value = (object) string.Empty;
        ((IValue) row[5]).Value = (object) 31;
        ((IValue) row[6]).Value = (object) false;
      }
      else
      {
        SQLParser.VariableDeclaration variableDeclaration = this.paramsList[this.currentParamPos];
        ((IValue) row[3]).Value = (object) (short) this.currentParamPos;
        ((IValue) row[4]).Value = (object) (64.ToString() + variableDeclaration.Name);
        ((IValue) row[5]).Value = (object) variableDeclaration.DataType;
        ((IValue) row[6]).Value = (object) (variableDeclaration.Direction != ParameterDirection.Input);
        if (variableDeclaration.Default != null)
        {
          string str = variableDeclaration.Default.ToString();
          if (string.IsNullOrEmpty(str))
            ((IValue) row[7]).Value = (object) (39.ToString() + (object) '\'');
          else if (str.Equals("<null>", StringComparison.OrdinalIgnoreCase))
            ((IValue) row[7]).Value = (object) "NULL";
          else
            ((IValue) row[7]).Value = variableDeclaration.DataType.ToString().EndsWith(VistaDBType.Char.ToString()) ? (object) (39.ToString() + str + (object) '\'') : (object) str;
        }
        else
          ((IValue) row[7]).Value = (object) string.Empty;
        if (++this.currentParamPos < this.paramsList.Count)
          return;
        this.currentParamPos = -1;
      }
    }

    public override bool First(IRow row)
    {
      this.enumerator.Reset();
      if (!this.enumerator.MoveNext())
        return false;
      this.FillRow(row, this.enumerator.Current as IStoredProcedureInformation);
      return true;
    }

    public override bool GetNextResult(IRow row)
    {
      if (this.currentParamPos < 0 && !this.enumerator.MoveNext())
        return false;
      this.FillRow(row, this.enumerator.Current as IStoredProcedureInformation);
      return true;
    }

    public override void Close()
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    protected override object ExecuteSubProgram()
    {
      this.enumerator = (IEnumerator) this.Parent.Database.GetStoredProcedures().GetEnumerator();
      return (object) null;
    }
  }
}
