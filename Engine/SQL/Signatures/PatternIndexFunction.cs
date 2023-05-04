using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class PatternIndexFunction : Function
  {
    private PatternFinder finder;

    public PatternIndexFunction(SQLParser parser)
      : base(parser, 2, true)
    {
      dataType = VistaDBType.Int;
      parameterTypes[0] = VistaDBType.NChar;
      parameterTypes[1] = VistaDBType.NChar;
      finder = null;
    }

    protected override object ExecuteSubProgram()
    {
      string matchExpr = (string)paramValues[1].Value;
      CreatePattern();
      return finder.Compare(matchExpr);
    }

    public override void SetChanged()
    {
      finder = null;
      base.SetChanged();
    }

    private void CreatePattern()
    {
      Signature signature = this[0];
      switch (signature.SignatureType)
      {
        case SignatureType.Constant:
        case SignatureType.Parameter:
          if (finder != null)
            return;
          break;
      }
      string pattern = (string)paramValues[0].Value;
      finder = new PatternFinder(signature.LineNo, signature.SymbolNo, pattern, null, parent.Connection);
    }
  }
}
