using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL.Signatures;

namespace VistaDB.Engine.SQL
{
  internal abstract class Join : IRowSet
  {
    protected Signature signature;
    protected IRowSet leftRowSet;
    protected IRowSet rightRowSet;

    protected Join(Signature signature, IRowSet leftRowSet, IRowSet rightRowSet)
    {
      this.signature = signature;
      this.leftRowSet = leftRowSet;
      this.rightRowSet = rightRowSet;
    }

    protected bool ExecuteRightRowSet(ConstraintOperations constraints)
    {
      while (this.rightRowSet.ExecuteRowset(constraints))
      {
        IColumn column = this.signature.Execute();
        if (!column.IsNull && (bool) ((IValue) column).Value)
          return true;
        if (!this.rightRowSet.Next(constraints))
          break;
      }
      return false;
    }

    protected abstract bool OnExecuteRowset(ConstraintOperations constraints);

    public bool Next(ConstraintOperations constraints)
    {
      if (!this.rightRowSet.Next(constraints))
        return this.leftRowSet.Next(constraints);
      return true;
    }

    public bool ExecuteRowset(ConstraintOperations constraints)
    {
      if (this.RowAvailable)
        return this.OnExecuteRowset(constraints);
      return false;
    }

    public void MarkRowNotAvailable()
    {
      this.leftRowSet.MarkRowNotAvailable();
      this.rightRowSet.MarkRowNotAvailable();
    }

    public bool IsEquals(IRowSet rowSet)
    {
      if (rowSet == null || this.GetType() != rowSet.GetType())
        return false;
      Join join = (Join) rowSet;
      if (this.leftRowSet.IsEquals(join.leftRowSet) && this.rightRowSet.IsEquals(join.rightRowSet))
        return this.signature == join.signature;
      return false;
    }

    public void Prepare()
    {
      if (this.signature != (Signature) null)
      {
        SignatureType signatureType = this.signature.Prepare();
        if (this.signature.DataType != VistaDBType.Bit)
          throw new VistaDBSQLException(557, "", this.signature.LineNo, this.signature.SymbolNo);
        if (signatureType == SignatureType.Constant && this.signature.SignatureType != SignatureType.Constant)
          this.signature = (Signature) ConstantSignature.CreateSignature(this.signature.Execute(), this.signature.Parent);
      }
      this.leftRowSet.Prepare();
      this.rightRowSet.Prepare();
    }

    public bool Optimize(ConstraintOperations constrainOperations)
    {
      if (this.signature != (Signature) null && this.signature.SignatureType != SignatureType.Constant && !this.signature.Optimize(constrainOperations) || constrainOperations == null)
        return false;
      int count1 = constrainOperations.Count;
      if (!this.leftRowSet.Optimize(constrainOperations))
        return false;
      if (count1 > 0 && constrainOperations.Count > count1)
        constrainOperations.AddLogicalAnd();
      int count2 = constrainOperations.Count;
      if (!this.rightRowSet.Optimize(constrainOperations))
        return false;
      if (count2 > 0 && constrainOperations.Count > count2)
        constrainOperations.AddLogicalAnd();
      return true;
    }

    public void SetUpdated()
    {
      if (this.signature != (Signature) null)
        this.signature.SetChanged();
      this.leftRowSet.SetUpdated();
      this.rightRowSet.SetUpdated();
    }

    public void ClearUpdated()
    {
      if (this.signature != (Signature) null)
        this.signature.ClearChanged();
      this.leftRowSet.ClearUpdated();
      this.rightRowSet.ClearUpdated();
    }

    public bool RowAvailable
    {
      get
      {
        if (!this.leftRowSet.RowAvailable)
          return this.rightRowSet.RowAvailable;
        return true;
      }
    }

    public bool RowUpdated
    {
      get
      {
        if (!this.leftRowSet.RowUpdated)
          return this.rightRowSet.RowUpdated;
        return true;
      }
    }

    public virtual bool OuterRow
    {
      get
      {
        return false;
      }
    }

    public virtual IRowSet PrepareTables(IVistaDBTableNameCollection tableNames, IViewList views, TableCollection tableList, bool alwaysAllowNull, ref int tableIndex)
    {
      this.leftRowSet = this.leftRowSet.PrepareTables(tableNames, views, tableList, alwaysAllowNull, ref tableIndex);
      this.rightRowSet = this.rightRowSet.PrepareTables(tableNames, views, tableList, alwaysAllowNull, ref tableIndex);
      return (IRowSet) this;
    }

    public IRowSet LeftRowSet
    {
      get
      {
        return this.leftRowSet;
      }
    }

    public IRowSet RightRowSet
    {
      get
      {
        return this.rightRowSet;
      }
    }
  }
}
