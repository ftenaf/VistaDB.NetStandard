using System.Collections;
using System.Collections.Generic;

namespace VistaDB.DDA
{
  public interface IVistaDBClrTriggerCollection : IVistaDBKeyedCollection<string, IVistaDBClrTriggerInformation>, ICollection<IVistaDBClrTriggerInformation>, IEnumerable<IVistaDBClrTriggerInformation>, IEnumerable
  {
  }
}
