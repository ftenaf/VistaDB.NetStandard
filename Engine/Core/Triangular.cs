namespace VistaDB.Engine.Core
{
  internal class Triangular
  {
    internal static Triangular.Value Not(Triangular.Value v)
    {
      if (v == Triangular.Value.Null)
        return v;
      return v == Triangular.Value.True ? Triangular.Value.False : Triangular.Value.True;
    }

    internal static Triangular.Value And(Triangular.Value a, Triangular.Value b)
    {
      if (a == Triangular.Value.False)
        return Triangular.Value.False;
      if (a == Triangular.Value.True || b != Triangular.Value.True)
        return b;
      return Triangular.Value.Null;
    }

    internal static Triangular.Value Or(Triangular.Value a, Triangular.Value b)
    {
      if (a == Triangular.Value.True)
        return Triangular.Value.True;
      if (a == Triangular.Value.False)
        return b;
      return b == Triangular.Value.True ? Triangular.Value.True : Triangular.Value.Null;
    }

    internal enum Value : byte
    {
      Null,
      Undefined,
      False,
      True,
    }
  }
}
