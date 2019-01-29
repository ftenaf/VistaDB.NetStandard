using VistaDB.DDA;

namespace VistaDB.Engine.Internal
{
  internal interface IView : IVistaDBDatabaseObject
  {
    string Expression { get; set; }

    new string Description { get; set; }
  }
}
