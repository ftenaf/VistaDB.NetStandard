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
      Signature signature = (Signature) new Plus(new string(Name), Group, unaryOffset);
      signature.Entry = Entry;
      return signature;
    }
  }
}
