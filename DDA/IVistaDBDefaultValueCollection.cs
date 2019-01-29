using System.Collections;
using System.Collections.Generic;

namespace VistaDB.DDA
{
  public interface IVistaDBDefaultValueCollection : IVistaDBKeyedCollection<string, IVistaDBDefaultValueInformation>, ICollection<IVistaDBDefaultValueInformation>, IEnumerable<IVistaDBDefaultValueInformation>, IEnumerable
  {
  }
}
