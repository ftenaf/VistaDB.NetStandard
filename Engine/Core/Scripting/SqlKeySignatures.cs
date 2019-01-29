namespace VistaDB.Engine.Core.Scripting
{
  internal class SqlKeySignatures : SignatureList
  {
    internal SqlKeySignatures()
    {
    }

    protected override void DoInitPatterns()
    {
      base.DoInitPatterns();
    }

    protected override void DoInitLanguageOperators()
    {
      Signature signature1 = (Signature) new Append(";", this.Count);
      signature1.Entry = this.Add(signature1);
      Signature signature2 = (Signature) new Descending("DESC", this.Count, this.PARENTHESIS + 1);
      signature2.Entry = this.Add(signature2);
    }

    protected override void DoInitMathOperators()
    {
    }

    protected override void DoInitUDFs()
    {
    }

    protected override void DoInitLogicalOperators()
    {
    }
  }
}
