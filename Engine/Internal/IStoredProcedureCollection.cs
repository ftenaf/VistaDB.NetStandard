using System.Collections;
using System.Collections.Generic;
using VistaDB.DDA;

namespace VistaDB.Engine.Internal
{
  internal interface IStoredProcedureCollection : IVistaDBKeyedCollection<string, IStoredProcedureInformation>, ICollection<IStoredProcedureInformation>, IEnumerable<IStoredProcedureInformation>, IEnumerable
  {
  }
}
