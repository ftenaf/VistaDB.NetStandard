using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL;

namespace VistaDB.Provider
{
  public class VistaDBPlanResultBlock : VistaDBPlanBlock
  {
    private VistaDBPlanResultBlock.ResultType resultType;
    private string queryText;

    internal VistaDBPlanResultBlock(IQueryStatement query, VistaDBPlanBlock[] childs)
      : base(VistaDBPlanBlock.BlockType.Result, childs)
    {
      this.resultType = VistaDBPlanResultBlock.GetResultType(query);
      this.queryText = query.CommandText;
    }

    private static VistaDBPlanResultBlock.ResultType GetResultType(IQueryStatement query)
    {
      if (query is SelectStatement)
        return VistaDBPlanResultBlock.ResultType.Select;
      if (query is InsertStatement)
        return VistaDBPlanResultBlock.ResultType.Insert;
      if (query is UpdateStatement)
        return VistaDBPlanResultBlock.ResultType.Update;
      return query is DeleteStatement ? VistaDBPlanResultBlock.ResultType.Delete : VistaDBPlanResultBlock.ResultType.Other;
    }

    public VistaDBPlanResultBlock.ResultType PlanResultType
    {
      get
      {
        return this.resultType;
      }
    }

    public string QueryText
    {
      get
      {
        return this.queryText;
      }
    }

    public enum ResultType
    {
      Select = 0,
      Insert = 1,
      Update = 2,
      Delete = 3,
      Other = 128, // 0x00000080
    }
  }
}
