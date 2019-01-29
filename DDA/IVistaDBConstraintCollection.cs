using System.Collections;
using System.Collections.Generic;

namespace VistaDB.DDA
{
  public interface IVistaDBConstraintCollection : IVistaDBKeyedCollection<string, IVistaDBConstraintInformation>, ICollection<IVistaDBConstraintInformation>, IEnumerable<IVistaDBConstraintInformation>, IEnumerable
  {
  }
}
