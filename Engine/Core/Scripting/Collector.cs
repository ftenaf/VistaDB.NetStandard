using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Collector : ProcedureCode
  {
    internal Collector()
    {
    }

    internal Row.Column ColumnResult
    {
      get
      {
        return this[0].ResultColumn;
      }
    }

    internal PCodeUnit ActivateNextRegister(PCodeUnit pcodeUnit)
    {
      PCodeUnit pcodeUnit1;
      if (++Iterator < Count)
      {
        pcodeUnit1 = this[Iterator];
      }
      else
      {
        pcodeUnit1 = new PCodeUnit(pcodeUnit);
        Add(pcodeUnit1);
      }
      pcodeUnit1.CopyFrom(pcodeUnit);
      return pcodeUnit1;
    }

    internal void ExecRegister(PCodeUnit pcodeUnit, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGoup, Row rowResult)
    {
      PCodeUnit pcodeUnit1 = ActivateNextRegister(pcodeUnit);
      Iterator -= pcodeUnit1.ParametersCount;
      Signature signature = pcodeUnit1.Signature;
      int iterator = Iterator;
      signature.Execute((ProcedureCode) this, Iterator, connection, contextStorage, contextRow, ref bypassNextGoup, rowResult);
      Iterator = iterator;
    }
  }
}
