namespace VistaDB.Provider
{
  public class VistaDBPlanTableBlock : VistaDBPlanBlock
  {
    private string tableName;
    private string indexName;
    private string joinedTable;

    internal VistaDBPlanTableBlock(string tableName, string indexName, string joinedTable)
      : base(VistaDBPlanBlock.BlockType.Table, (VistaDBPlanBlock[]) null)
    {
      this.tableName = tableName;
      this.indexName = indexName;
      this.joinedTable = joinedTable;
    }

    public string TableName
    {
      get
      {
        return this.tableName;
      }
    }

    public string IndexName
    {
      get
      {
        return this.indexName;
      }
    }

    public string JoinedTable
    {
      get
      {
        return this.joinedTable;
      }
    }
  }
}
