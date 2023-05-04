using System.Collections;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class SpecialFunction : Function, ITableValuedFunction
  {
    protected VistaDBType[] resultColumnTypes;
    protected string[] resultColumnNames;
    protected IEnumerator enumerator;

    public SpecialFunction(SQLParser parser, int paramCount, int columnCount)
      : base(parser, paramCount, false)
    {
      resultColumnTypes = new VistaDBType[columnCount];
      resultColumnNames = new string[columnCount];
      skipNull = false;
    }

    public VistaDBType[] GetResultColumnTypes()
    {
      return resultColumnTypes;
    }

    public string[] GetResultColumnNames()
    {
      return resultColumnNames;
    }

    public void Open()
    {
      object resValue;
      PrepareExecute(out resValue);
    }

    public abstract bool First(IRow row);

    public abstract bool GetNextResult(IRow row);

    public abstract void Close();
  }
}
