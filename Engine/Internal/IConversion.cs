namespace VistaDB.Engine.Internal
{
  internal interface IConversion
  {
    void Convert(IValue sourceValue, IValue destinationValue);

    bool ExistConvertion(VistaDBType srcType, VistaDBType dstType);
  }
}
