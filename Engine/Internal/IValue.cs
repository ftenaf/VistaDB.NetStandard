namespace VistaDB.Engine.Internal
{
  internal interface IValue : IVistaDBValue
  {
    new object Value { get; set; }

    object TrimmedValue { get; }

    VistaDBType InternalType { get; }
  }
}
