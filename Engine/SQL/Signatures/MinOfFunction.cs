namespace VistaDB.Engine.SQL.Signatures
{
  internal class MinOfFunction : MaxOfFunction
  {
    public MinOfFunction(SQLParser parser)
      : base(parser)
    {
    }

    protected override bool AcceptValue(int result)
    {
      return result < 0;
    }
  }
}
