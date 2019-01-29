using System.Collections;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpStoredFunction : SpStoredProcedures
  {
    internal SpStoredFunction(SQLParser parser)
      : base(parser, 9)
    {
      this.resultColumnTypes[8] = VistaDBType.Int;
      this.resultColumnNames[8] = "RETURN_TYPE";
    }

    protected override int ParseProcedure(string procStatement)
    {
      VistaDBType resultType;
      CreateTableStatement resultTableStatement;
      this.curentProcStatement = (Statement) this.parent.Connection.CreateStoredFunctionStatement(this.parent, procStatement, out resultType, out this.paramsList, out resultTableStatement);
      this.currentParamPos = 0;
      return (int) resultType;
    }

    protected override object ExecuteSubProgram()
    {
      this.enumerator = (IEnumerator) this.Parent.Database.GetUserDefinedFunctions().GetEnumerator();
      return (object) null;
    }

    protected override void FillRow(IRow row, IStoredProcedureInformation sp)
    {
      base.FillRow(row, sp);
      ((IValue) row[8]).Value = (object) this.resultType;
    }
  }
}
