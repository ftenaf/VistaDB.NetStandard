using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL;

namespace VistaDB.Provider
{
  public class VistaDBPlanResultBlock : VistaDBPlanBlock
  {
    private ResultType resultType;
    private string queryText;

    internal VistaDBPlanResultBlock(IQueryStatement query, VistaDBPlanBlock[] childs)
      : base(BlockType.Result, childs)
    {
      resultType = GetResultType(query);
      queryText = query.CommandText;
    }

    private static ResultType GetResultType(IQueryStatement query)
    {
      if (query is SelectStatement)
        return ResultType.Select;
      if (query is InsertStatement)
        return ResultType.Insert;
      if (query is UpdateStatement)
        return ResultType.Update;
      return query is DeleteStatement ? ResultType.Delete : ResultType.Other;
    }

    public ResultType PlanResultType
    {
      get
      {
        return resultType;
      }
    }

    public string QueryText
    {
      get
      {
        return queryText;
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
