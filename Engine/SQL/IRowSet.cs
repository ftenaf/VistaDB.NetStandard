using VistaDB.DDA;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL
{
  internal interface IRowSet
  {
    bool Next(ConstraintOperations constraints);

    bool ExecuteRowset(ConstraintOperations constraints);

    bool IsEquals(IRowSet rowSet);

    void Prepare();

    bool Optimize(ConstraintOperations constrainOperations);

    void SetUpdated();

    void ClearUpdated();

    bool RowUpdated { get; }

    void MarkRowNotAvailable();

    bool RowAvailable { get; }

    bool OuterRow { get; }

    IRowSet PrepareTables(IVistaDBTableNameCollection tableNames, IViewList views, TableCollection tableList, bool alwaysAllowNull, ref int tableIndex);
  }
}
