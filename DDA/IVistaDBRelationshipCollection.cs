using System.Collections;
using System.Collections.Generic;

namespace VistaDB.DDA
{
  public interface IVistaDBRelationshipCollection : IVistaDBKeyedCollection<string, IVistaDBRelationshipInformation>, ICollection<IVistaDBRelationshipInformation>, IEnumerable<IVistaDBRelationshipInformation>, IEnumerable
  {
  }
}
