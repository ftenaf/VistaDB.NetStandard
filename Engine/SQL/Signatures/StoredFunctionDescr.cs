using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class StoredFunctionDescr : FunctionDescr
  {
    internal IUserDefinedFunctionInformation storedFunction;

    internal StoredFunctionDescr(IUserDefinedFunctionInformation sp)
    {
      this.storedFunction = sp;
    }

    public override Signature CreateSignature(SQLParser parser)
    {
      return (Signature) new StoredFunction(parser, this.storedFunction);
    }
  }
}
