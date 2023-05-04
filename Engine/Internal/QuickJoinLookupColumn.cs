using VistaDB.Engine.SQL;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.Internal
{
  internal class QuickJoinLookupColumn : ColumnSignature
  {
    private readonly KeyedLookupTable lookupTable;
    private readonly int dataIndex;
    private long dataVersion;

    internal QuickJoinLookupColumn(ColumnSignature originalColumn, Statement parent, KeyedLookupTable lookupTable, int dataIndex)
      : base(originalColumn.Table, originalColumn.ColumnIndex, parent)
    {
      this.lookupTable = lookupTable;
      this.dataIndex = dataIndex;
      dataVersion = -1L;
      optimizable = false;
    }

    protected override bool InternalGetIsChanged()
    {
      if (dataVersion >= 0L)
        return dataVersion != lookupTable.TableVersion;
      return true;
    }

    protected override IColumn InternalExecute()
    {
      if (InternalGetIsChanged())
      {
        ((IValue) result).Value = lookupTable.GetValue(dataIndex);
        dataVersion = lookupTable.TableVersion;
      }
      return result;
    }
  }
}
