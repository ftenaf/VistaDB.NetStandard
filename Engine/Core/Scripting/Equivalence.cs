using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Equivalence : Signature
  {
    internal Equivalence(string name, int groupId, Signature.Operations operation)
      : base(name, groupId, operation, Signature.Priorities.Comparing, VistaDBType.Bit)
    {
      this.allowUnaryToFollow = true;
      this.AddParameter(VistaDBType.Unknown);
      this.AddParameter(VistaDBType.Unknown);
    }

    protected long ImplicitCompare(Row.Column column1, Row.Column column2, DataStorage contextStorage)
    {
      CrossConversion conversion = contextStorage.Conversion;
      if (column1.Type == column2.Type)
        return (long) column1.MinusColumn(column2);
      Row.Column b = column1.Duplicate(false);
      conversion.Convert((IValue) column2, (IValue) b);
      return (long) column1.MinusColumn(b);
    }
  }
}
