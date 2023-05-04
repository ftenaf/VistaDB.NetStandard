using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Summation : Signature
  {
    internal Summation(string name, int groupId, int unaryOffset)
      : base(name, groupId, Operations.Nomark, Priorities.Summation, VistaDBType.Unknown)
    {
      this.unaryOffset = unaryOffset;
      AddParameter(VistaDBType.Unknown);
      AddParameter(VistaDBType.Unknown);
    }

    internal static VistaDBType ReturnVistaDBType(VistaDBType left, VistaDBType right)
    {
      if (Row.Column.Rank(left) == Row.Column.ArithmeticRank.Unsupported || Row.Column.Rank(right) == Row.Column.ArithmeticRank.Unsupported)
        throw new VistaDBException(288);
      if (Row.Column.Rank(left) < Row.Column.Rank(right))
        return right;
      return left;
    }

    protected override void OnFixReturnTypeAndParameters(Collector collector, int offset, VistaDBType newType)
    {
      VistaDBType returnType1 = collector[offset].Signature.ReturnType;
      VistaDBType returnType2 = collector[offset + 1].Signature.ReturnType;
      SetParameterType(0, returnType1);
      SetParameterType(1, returnType2);
      base.OnFixReturnTypeAndParameters(collector, offset, ReturnVistaDBType(returnType1, returnType2));
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      pcode[entry].ResultColumn += pcode[entry + 1].ResultColumn;
    }
  }
}
