using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Equivalence : Signature
  {
    internal Equivalence(string name, int groupId, Operations operation)
      : base(name, groupId, operation, Priorities.Comparing, VistaDBType.Bit)
    {
      allowUnaryToFollow = true;
      AddParameter(VistaDBType.Unknown);
      AddParameter(VistaDBType.Unknown);
    }

    protected long ImplicitCompare(Row.Column column1, Row.Column column2, DataStorage contextStorage)
    {
      CrossConversion conversion = contextStorage.Conversion;
      if (column1.Type == column2.Type)
        return column1.MinusColumn(column2);
      Row.Column b = column1.Duplicate(false);
      conversion.Convert(column2, b);
      return column1.MinusColumn(b);
    }
  }
}
