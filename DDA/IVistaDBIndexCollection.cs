using System.Collections;
using System.Collections.Generic;

namespace VistaDB.DDA
{
  public interface IVistaDBIndexCollection : IVistaDBKeyedCollection<string, IVistaDBIndexInformation>, ICollection<IVistaDBIndexInformation>, IEnumerable<IVistaDBIndexInformation>, IEnumerable
  {
  }
}
