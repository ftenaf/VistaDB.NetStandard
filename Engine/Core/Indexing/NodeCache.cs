using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Indexing
{
  internal class NodeCache : WeakReferenceCache<ulong, Node>
  {
    internal NodeCache()
      : base(20)
    {
    }
  }
}
