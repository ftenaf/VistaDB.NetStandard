namespace VistaDB.Engine.Core.Scripting
{
  internal class Plus : Summation
  {
    internal Plus(string name, int groupId, int unaryOffset)
      : base(name, groupId, unaryOffset)
    {
    }

    internal override Signature DoCloneSignature()
    {
      Signature signature = (Signature) new Plus(new string(this.Name), this.Group, this.unaryOffset);
      signature.Entry = this.Entry;
      return signature;
    }
  }
}
