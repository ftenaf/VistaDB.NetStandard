using System.Collections;
using System.Collections.Generic;

namespace VistaDB.DDA
{
  public interface IVistaDBIdentityCollection : IVistaDBKeyedCollection<string, IVistaDBIdentityInformation>, ICollection<IVistaDBIdentityInformation>, IEnumerable<IVistaDBIdentityInformation>, IEnumerable
  {
  }
}
