using System;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class GetDateFunction : Function
  {
    private DateTime value = DateTime.MinValue;

    public GetDateFunction(SQLParser parser)
      : base(parser, 0, true)
    {
      dataType = VistaDBType.DateTime;
    }

    protected override object ExecuteSubProgram()
    {
      if (value == DateTime.MinValue)
        value = EvaluateCurrentMoment();
      return value;
    }

    protected virtual DateTime EvaluateCurrentMoment()
    {
      return DateTime.Now;
    }

    public override SignatureType OnPrepare()
    {
      return SignatureType.Expression;
    }
  }
}
