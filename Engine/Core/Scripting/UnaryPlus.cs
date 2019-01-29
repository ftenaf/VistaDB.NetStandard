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
      Signature signature = (Signature) new UnaryPlus(new string(this.Name), this.Group);
      signature.Entry = this.Entry;
      return signature;
    }
  }
}
