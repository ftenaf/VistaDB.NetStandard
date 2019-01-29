using System.Collections;
using System.Collections.Generic;
using VistaDB.DDA;

namespace VistaDB.Engine.Internal
{
  internal interface IUserDefinedFunctionCollection : IVistaDBKeyedCollection<string, IUserDefinedFunctionInformation>, ICollection<IUserDefinedFunctionInformation>, IEnumerable<IUserDefinedFunctionInformation>, IEnumerable
  {
  }
}
