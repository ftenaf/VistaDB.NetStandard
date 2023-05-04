namespace VistaDB.Engine.Core
{
  internal class TemporaryTable : Table
  {
    internal static TemporaryTable CreateInstance(string name, Database parentDatabase, Row parentTableSchema)
    {
      return new TemporaryTable(TemporaryRowset.CreateInstance(name, parentDatabase, parentTableSchema), parentDatabase);
    }

    protected TemporaryTable(TemporaryRowset rowset, Database parentDatabase)
      : base(rowset, parentDatabase)
    {
    }

    protected override bool IsAllowPooling()
    {
      return false;
    }
  }
}
