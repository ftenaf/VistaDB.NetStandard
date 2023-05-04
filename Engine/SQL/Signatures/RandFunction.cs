using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class RandFunction : Function
  {
    private Random rnd;
    private bool getChanged;
    private static int instanceCounter;

    public RandFunction(SQLParser parser)
      : base(parser, -1, true)
    {
            instanceCounter += 1000;
      getChanged = true;
      if (ParamCount > 1)
        throw new VistaDBSQLException(501, "RAND", lineNo, symbolNo);
      dataType = VistaDBType.Float;
      if (ParamCount == 1)
      {
        parameterTypes[0] = VistaDBType.Int;
        rnd = null;
      }
      else
        rnd = new Random(instanceCounter);
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      if (ParamCount == 0)
        return SignatureType.Expression;
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      getChanged = true;
      if (ParamCount > 0)
        return new Random((int)paramValues[0].Value).NextDouble();
      return rnd.NextDouble();
    }

    protected override bool InternalGetIsChanged()
    {
      if (ParamCount <= 0)
        return getChanged;
      return this[0].GetIsChanged();
    }
  }
}
