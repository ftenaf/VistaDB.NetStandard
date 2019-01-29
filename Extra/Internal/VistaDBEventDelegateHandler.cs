using System;
using VistaDB.DDA;

namespace VistaDB.Extra.Internal
{
  internal class VistaDBEventDelegateHandler : IVistaDBDDAEventDelegate
  {
    public DDAEventDelegateType Type
    {
      get
      {
        throw new Exception("The method or operation is not implemented.");
      }
    }

    public DDAEventDelegate EventDelegate
    {
      get
      {
        throw new Exception("The method or operation is not implemented.");
      }
    }

    public object UsersData
    {
      get
      {
        throw new Exception("The method or operation is not implemented.");
      }
    }
  }
}
