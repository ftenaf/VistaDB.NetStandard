using VistaDB.DDA;

namespace VistaDB.Engine.Internal
{
  internal interface IUserDefinedFunctionInformation : IStoredProcedureInformation, IVistaDBDatabaseObject
  {
    bool ScalarValued { get; }
  }
}
