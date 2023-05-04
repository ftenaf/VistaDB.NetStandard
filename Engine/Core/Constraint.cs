using VistaDB.Engine.Core.Scripting;

namespace VistaDB.Engine.Core
{
  internal class Constraint : Filter
  {
    private string name;

    internal static int MakeStatus(bool insertion, bool update, bool delete)
    {
      int num = 0;
      if (insertion)
        num = 1;
      if (update)
        num |= 2;
      if (delete)
        num |= 4;
      return num;
    }

    internal static bool UpdateActivity(int option)
    {
      return (option & 2) == 2;
    }

    internal static bool DeleteActivity(int option)
    {
      return (option & 4) == 4;
    }

    internal static bool InsertionActivity(int option)
    {
      return (option & 1) == 1;
    }

    internal Constraint(string name, EvalStack evaluation, FilterType typeId)
      : base(evaluation, typeId, true, true, (int) typeId)
    {
      this.name = name;
    }

    internal string Name
    {
      get
      {
        return name;
      }
    }

    internal enum Activity
    {
      Insert = 1,
      Update = 2,
      Delete = 4,
    }
  }
}
