using System.Collections;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpStoredFunction : SpStoredProcedures
  {
    internal SpStoredFunction(SQLParser parser)
      : base(parser, 9)
    {
      resultColumnTypes[8] = VistaDBType.Int;
      resultColumnNames[8] = "RETURN_TYPE";
    }

    protected override int ParseProcedure(string procStatement)
    {
      VistaDBType resultType;
      CreateTableStatement resultTableStatement;
      curentProcStatement = (Statement) parent.Connection.CreateStoredFunctionStatement(parent, procStatement, out resultType, out paramsList, out resultTableStatement);
      currentParamPos = 0;
      return (int) resultType;
    }

    protected override object ExecuteSubProgram()
    {
      enumerator = (IEnumerator) Parent.Database.GetUserDefinedFunctions().GetEnumerator();
      return (object) null;
    }

    protected override void FillRow(IRow row, IStoredProcedureInformation sp)
    {
      base.FillRow(row, sp);
      ((IValue) row[8]).Value = (object) resultType;
    }
  }
}
