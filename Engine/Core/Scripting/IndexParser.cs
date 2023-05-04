using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class IndexParser : Parser
  {
    internal IndexParser(DirectConnection connection)
      : base(connection)
    {
    }

    protected override SignatureList DoCreateSignatures()
    {
      return new SqlKeySignatures();
    }

    protected override bool OnParseOperands()
    {
      return base.OnParseOperands();
    }

    protected override bool OnParsePatterns()
    {
      return base.OnParsePatterns();
    }

    protected override EvalStack OnCreateEvalStackInstance(DirectConnection connection, DataStorage activeStorage)
    {
      return new StandardIndexEvalStack(connection, activeStorage);
    }
  }
}
