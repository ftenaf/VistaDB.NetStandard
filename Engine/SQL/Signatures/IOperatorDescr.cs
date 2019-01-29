namespace VistaDB.Engine.SQL.Signatures
{
  internal interface IOperatorDescr
  {
    Signature CreateSignature(Signature leftSignature, SQLParser parser);

    int Priority { get; }
  }
}
