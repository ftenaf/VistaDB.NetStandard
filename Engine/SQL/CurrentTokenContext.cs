namespace VistaDB.Engine.SQL
{
  internal class CurrentTokenContext
  {
    private string contextName;
    private CurrentTokenContext.TokenContext context;

    internal CurrentTokenContext(CurrentTokenContext.TokenContext context, string contextName)
    {
      this.context = context;
      this.contextName = contextName;
    }

    internal CurrentTokenContext.TokenContext ContextType
    {
      get
      {
        return this.context;
      }
    }

    internal string ContextName
    {
      get
      {
        return this.contextName;
      }
    }

    internal enum TokenContext
    {
      UsualText,
      StoredProcedure,
      StoredFunction,
    }
  }
}
