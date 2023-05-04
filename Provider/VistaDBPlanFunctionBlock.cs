namespace VistaDB.Provider
{
  public class VistaDBPlanFunctionBlock : VistaDBPlanBlock
  {
    private string functionName;

    internal VistaDBPlanFunctionBlock(string functionName)
      : base(BlockType.Function, (VistaDBPlanBlock[]) null)
    {
      this.functionName = functionName;
    }

    public string FunctionName
    {
      get
      {
        return functionName;
      }
    }
  }
}
