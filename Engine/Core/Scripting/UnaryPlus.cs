namespace VistaDB.Engine.Core.Scripting
{
  internal class UnaryPlus : Unary
  {
    internal UnaryPlus(string name, int groupId)
      : base(name, groupId)
    {
    }

    internal override Signature DoCloneSignature()
    {
      Signature signature = new UnaryPlus(new string(Name), Group);
      signature.Entry = Entry;
      return signature;
    }
  }
}
