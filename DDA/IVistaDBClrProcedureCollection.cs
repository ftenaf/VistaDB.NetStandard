using System.Collections;
using System.Collections.Generic;

namespace VistaDB.DDA
{
  public interface IVistaDBClrProcedureCollection : IVistaDBKeyedCollection<string, IVistaDBClrProcedureInformation>, ICollection<IVistaDBClrProcedureInformation>, IEnumerable<IVistaDBClrProcedureInformation>, IEnumerable
  {
  }
}
