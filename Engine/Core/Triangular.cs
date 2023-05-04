namespace VistaDB.Engine.Core
{
  internal class Triangular
  {
    internal static Value Not(Value v)
    {
      if (v == Value.Null)
        return v;
      return v == Value.True ? Value.False : Value.True;
    }

    internal static Value And(Value a, Value b)
    {
      if (a == Value.False)
        return Value.False;
      if (a == Value.True || b != Value.True)
        return b;
      return Value.Null;
    }

    internal static Value Or(Value a, Value b)
    {
      if (a == Value.True)
        return Value.True;
      if (a == Value.False)
        return b;
      return b == Value.True ? Value.True : Value.Null;
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
