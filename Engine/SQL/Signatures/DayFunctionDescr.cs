namespace VistaDB.Engine.SQL.Signatures
{
  internal class DayFunctionDescr : FunctionDescr
  {
    public override Signature CreateSignature(SQLParser parser)
    {
      return new DayFunction(parser);
    }
  }
}
