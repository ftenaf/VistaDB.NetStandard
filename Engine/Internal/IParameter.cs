using System.Data;

namespace VistaDB.Engine.Internal
{
  internal interface IParameter
  {
    object Value { get; set; }

    VistaDBType DataType { get; set; }

    ParameterDirection Direction { get; set; }
  }
}
