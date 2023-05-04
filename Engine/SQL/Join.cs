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
      while (rightRowSet.ExecuteRowset(constraints))
      {
        IColumn column = signature.Execute();
        if (!column.IsNull && (bool) ((IValue) column).Value)
          return true;
        if (!rightRowSet.Next(constraints))
          break;
      }
      return false;
    }

    protected abstract bool OnExecuteRowset(ConstraintOperations constraints);

    public bool Next(ConstraintOperations constraints)
    {
      if (!rightRowSet.Next(constraints))
        return leftRowSet.Next(constraints);
      return true;
    }

    public bool ExecuteRowset(ConstraintOperations constraints)
    {
      if (RowAvailable)
        return OnExecuteRowset(constraints);
      return false;
    }

    public void MarkRowNotAvailable()
    {
      leftRowSet.MarkRowNotAvailable();
      rightRowSet.MarkRowNotAvailable();
    }

    public bool IsEquals(IRowSet rowSet)
    {
      if (rowSet == null || GetType() != rowSet.GetType())
        return false;
      Join join = (Join) rowSet;
      if (leftRowSet.IsEquals(join.leftRowSet) && rightRowSet.IsEquals(join.rightRowSet))
        return signature == join.signature;
      return false;
    }

    public void Prepare()
    {
      if (signature != (Signature) null)
      {
        SignatureType signatureType = signature.Prepare();
        if (signature.DataType != VistaDBType.Bit)
          throw new VistaDBSQLException(557, "", signature.LineNo, signature.SymbolNo);
        if (signatureType == SignatureType.Constant && signature.SignatureType != SignatureType.Constant)
          signature = (Signature) ConstantSignature.CreateSignature(signature.Execute(), signature.Parent);
      }
      leftRowSet.Prepare();
      rightRowSet.Prepare();
    }

    public bool Optimize(ConstraintOperations constrainOperations)
    {
      if (signature != (Signature) null && signature.SignatureType != SignatureType.Constant && !signature.Optimize(constrainOperations) || constrainOperations == null)
        return false;
      int count1 = constrainOperations.Count;
      if (!leftRowSet.Optimize(constrainOperations))
        return false;
      if (count1 > 0 && constrainOperations.Count > count1)
        constrainOperations.AddLogicalAnd();
      int count2 = constrainOperations.Count;
      if (!rightRowSet.Optimize(constrainOperations))
        return false;
      if (count2 > 0 && constrainOperations.Count > count2)
        constrainOperations.AddLogicalAnd();
      return true;
    }

    public void SetUpdated()
    {
      if (signature != (Signature) null)
        signature.SetChanged();
      leftRowSet.SetUpdated();
      rightRowSet.SetUpdated();
    }

    public void ClearUpdated()
    {
      if (signature != (Signature) null)
        signature.ClearChanged();
      leftRowSet.ClearUpdated();
      rightRowSet.ClearUpdated();
    }

    public bool RowAvailable
    {
      get
      {
        if (!leftRowSet.RowAvailable)
          return rightRowSet.RowAvailable;
        return true;
      }
    }

    public bool RowUpdated
    {
      get
      {
        if (!leftRowSet.RowUpdated)
          return rightRowSet.RowUpdated;
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
      leftRowSet = leftRowSet.PrepareTables(tableNames, views, tableList, alwaysAllowNull, ref tableIndex);
      rightRowSet = rightRowSet.PrepareTables(tableNames, views, tableList, alwaysAllowNull, ref tableIndex);
      return (IRowSet) this;
    }

    public IRowSet LeftRowSet
    {
      get
      {
        return leftRowSet;
      }
    }

    public IRowSet RightRowSet
    {
      get
      {
        return rightRowSet;
      }
    }
  }
}
