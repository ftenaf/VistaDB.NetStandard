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
      Signature signature1 = new Append(";", Count);
      signature1.Entry = Add(signature1);
      Signature signature2 = new Descending("DESC", Count, PARENTHESIS + 1);
      signature2.Entry = Add(signature2);
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
