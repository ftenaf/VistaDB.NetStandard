using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class PatternIndexFunction : Function
  {
    private PatternFinder finder;

    public PatternIndexFunction(SQLParser parser)
      : base(parser, 2, true)
    {
      this.dataType = VistaDBType.Int;
      this.parameterTypes[0] = VistaDBType.NChar;
      this.parameterTypes[1] = VistaDBType.NChar;
      this.finder = (PatternFinder) null;
    }

    protected override object ExecuteSubProgram()
    {
      string matchExpr = (string) ((IValue) this.paramValues[1]).Value;
      this.CreatePattern();
      return (object) this.finder.Compare(matchExpr);
    }

    public override void SetChanged()
    {
      this.finder = (PatternFinder) null;
      base.SetChanged();
    }

    private void CreatePattern()
    {
      Signature signature = this[0];
      switch (signature.SignatureType)
      {
        case SignatureType.Constant:
        case SignatureType.Parameter:
          if (this.finder != null)
            return;
          break;
      }
      string pattern = (string) ((IValue) this.paramValues[0]).Value;
      this.finder = new PatternFinder(signature.LineNo, signature.SymbolNo, pattern, (string) null, this.parent.Connection);
    }
  }
}
