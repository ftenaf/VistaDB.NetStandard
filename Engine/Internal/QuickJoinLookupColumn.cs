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
      this.dataVersion = -1L;
      this.optimizable = false;
    }

    protected override bool InternalGetIsChanged()
    {
      if (this.dataVersion >= 0L)
        return this.dataVersion != this.lookupTable.TableVersion;
      return true;
    }

    protected override IColumn InternalExecute()
    {
      if (this.InternalGetIsChanged())
      {
        ((IValue) this.result).Value = this.lookupTable.GetValue(this.dataIndex);
        this.dataVersion = this.lookupTable.TableVersion;
      }
      return this.result;
    }
  }
}
