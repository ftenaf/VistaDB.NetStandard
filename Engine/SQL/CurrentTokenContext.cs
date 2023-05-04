namespace VistaDB.Engine.SQL
{
  internal class CurrentTokenContext
  {
    private string contextName;
    private TokenContext context;

    internal CurrentTokenContext(TokenContext context, string contextName)
    {
      this.context = context;
      this.contextName = contextName;
    }

    internal TokenContext ContextType
    {
      get
      {
        return context;
      }
    }

    internal string ContextName
    {
      get
      {
        return contextName;
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
