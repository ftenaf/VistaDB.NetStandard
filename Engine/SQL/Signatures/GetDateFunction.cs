using System;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class GetDateFunction : Function
  {
    private DateTime value = DateTime.MinValue;

    public GetDateFunction(SQLParser parser)
      : base(parser, 0, true)
    {
      this.dataType = VistaDBType.DateTime;
    }

    protected override object ExecuteSubProgram()
    {
      if (this.value == DateTime.MinValue)
        this.value = this.EvaluateCurrentMoment();
      return (object) this.value;
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
