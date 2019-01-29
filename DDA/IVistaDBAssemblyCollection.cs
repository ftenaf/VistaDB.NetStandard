using System.Collections;
using System.Collections.Generic;

namespace VistaDB.DDA
{
  public interface IVistaDBAssemblyCollection : IVistaDBKeyedCollection<string, IVistaDBAssemblyInformation>, ICollection<IVistaDBAssemblyInformation>, IEnumerable<IVistaDBAssemblyInformation>, IEnumerable
  {
  }
}
