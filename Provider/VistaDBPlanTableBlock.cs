namespace VistaDB.Provider
{
  public class VistaDBPlanTableBlock : VistaDBPlanBlock
  {
    private string tableName;
    private string indexName;
    private string joinedTable;

    internal VistaDBPlanTableBlock(string tableName, string indexName, string joinedTable)
      : base(BlockType.Table, (VistaDBPlanBlock[]) null)
    {
      this.tableName = tableName;
      this.indexName = indexName;
      this.joinedTable = joinedTable;
    }

    public string TableName
    {
      get
      {
        return tableName;
      }
    }

    public string IndexName
    {
      get
      {
        return indexName;
      }
    }

    public string JoinedTable
    {
      get
      {
        return joinedTable;
      }
    }
  }
}
