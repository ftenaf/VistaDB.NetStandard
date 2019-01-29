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
      if (++this.Iterator < this.Count)
      {
        pcodeUnit1 = this[this.Iterator];
      }
      else
      {
        pcodeUnit1 = new PCodeUnit(pcodeUnit);
        this.Add(pcodeUnit1);
      }
      pcodeUnit1.CopyFrom(pcodeUnit);
      return pcodeUnit1;
    }

    internal void ExecRegister(PCodeUnit pcodeUnit, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGoup, Row rowResult)
    {
      PCodeUnit pcodeUnit1 = this.ActivateNextRegister(pcodeUnit);
      this.Iterator -= pcodeUnit1.ParametersCount;
      Signature signature = pcodeUnit1.Signature;
      int iterator = this.Iterator;
      signature.Execute((ProcedureCode) this, this.Iterator, connection, contextStorage, contextRow, ref bypassNextGoup, rowResult);
      this.Iterator = iterator;
    }
  }
}
