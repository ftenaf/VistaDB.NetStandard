using VistaDB.DDA;

namespace VistaDB.Engine.Internal
{
  internal interface IStoredProcedureInformation : IVistaDBDatabaseObject
  {
    string Statement { get; }

    byte[] Serialize();
  }
}
