using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal interface IValueList
  {
    bool IsValuePresent(IColumn val);
  }
}
