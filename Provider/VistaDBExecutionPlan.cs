using VistaDB.Engine.Internal;

namespace VistaDB.Provider
{
  public class VistaDBExecutionPlan
  {
    private VistaDBPlanBlock block;

    internal VistaDBExecutionPlan(IQueryStatement query)
    {
      this.block = VistaDBPlanBlock.CreateExecutionPlan(query);
    }

    public VistaDBPlanBlock FirstBlock
    {
      get
      {
        return this.block;
      }
    }
  }
}
