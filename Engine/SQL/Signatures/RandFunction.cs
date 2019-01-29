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
      RandFunction.instanceCounter += 1000;
      this.getChanged = true;
      if (this.ParamCount > 1)
        throw new VistaDBSQLException(501, "RAND", this.lineNo, this.symbolNo);
      this.dataType = VistaDBType.Float;
      if (this.ParamCount == 1)
      {
        this.parameterTypes[0] = VistaDBType.Int;
        this.rnd = (Random) null;
      }
      else
        this.rnd = new Random(RandFunction.instanceCounter);
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = base.OnPrepare();
      if (this.ParamCount == 0)
        return SignatureType.Expression;
      return signatureType;
    }

    protected override object ExecuteSubProgram()
    {
      this.getChanged = true;
      if (this.ParamCount > 0)
        return (object) new Random((int) ((IValue) this.paramValues[0]).Value).NextDouble();
      return (object) this.rnd.NextDouble();
    }

    protected override bool InternalGetIsChanged()
    {
      if (this.ParamCount <= 0)
        return this.getChanged;
      return this[0].GetIsChanged();
    }
  }
}
