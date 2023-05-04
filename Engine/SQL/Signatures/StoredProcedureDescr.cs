using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class StoredProcedureDescr : FunctionDescr
  {
    internal IStoredProcedureInformation sp;

    internal StoredProcedureDescr(IStoredProcedureInformation sp)
    {
      this.sp = sp;
    }

    public override Signature CreateSignature(SQLParser parser)
    {
      return new StoredProcedure(parser, sp);
    }
  }
}
