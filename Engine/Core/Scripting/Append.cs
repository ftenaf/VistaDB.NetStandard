using System.Collections.Generic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
  internal class Append : Signature
  {
    private List<Row.Column> tmpList;

    internal Append(string name, int groupId)
      : base(name, groupId, Operations.Nomark, Priorities.Endline, VistaDBType.Unknown)
    {
      AddParameter(VistaDBType.Unknown);
      AddParameter(VistaDBType.Unknown);
      tmpList = new List<Row.Column>(64);
    }

    protected override void OnExecute(ProcedureCode pcode, int entry, Connection connection, DataStorage contextStorage, Row contextRow, ref bool bypassNextGroup, Row rowResult)
    {
      PCodeUnit pcodeUnit1 = pcode[entry];
      PCodeUnit pcodeUnit2 = pcode[entry + 1];
      if (rowResult == null)
      {
        bool flag1 = (bool) pcodeUnit1.ResultColumn.Value;
        bool flag2 = (bool) pcodeUnit2.ResultColumn.Value;
        pcodeUnit1.ResultColumn.Value = !flag1 ? false : (flag2 ? true : false);
      }
      else
      {
        Row resultRow1 = pcodeUnit1.ResultRow;
        Row resultRow2 = pcodeUnit2.ResultRow;
        tmpList.Clear();
        if (resultRow1 != null)
        {
          foreach (Row.Column column in (List<Row.Column>) resultRow1)
            tmpList.Add(column);
        }
        else
          tmpList.Add(pcodeUnit1.ResultColumn);
        if (resultRow2 != null)
        {
          foreach (Row.Column column in (List<Row.Column>) resultRow2)
            tmpList.Add(column);
        }
        else
          tmpList.Add(pcodeUnit2.ResultColumn);
        rowResult.Clear();
        foreach (Row.Column tmp in tmpList)
          rowResult.AppendColumn((IColumn) tmp);
        pcodeUnit1.ResultRow = rowResult;
      }
    }
  }
}
