namespace VistaDB.Engine.Core
{
  internal class TransactionLogTable : Table
  {
    internal static TransactionLogTable CreateInstance(Database parentDatabase, string rowsetName)
    {
      return new TransactionLogTable(TransactionLogRowset.CreateInstance(parentDatabase, rowsetName), parentDatabase);
    }

    private TransactionLogTable(TransactionLogRowset rowset, Database parentDatabase)
      : base((ClusteredRowset) rowset, parentDatabase)
    {
    }

    internal TransactionLogRowset Rowset
    {
      get
      {
        return (TransactionLogRowset) base.Rowset;
      }
    }
  }
}
