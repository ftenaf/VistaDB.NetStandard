using VistaDB.Engine.Core.Scripting;

namespace VistaDB.Engine.Core
{
  internal class Filter
  {
    private EvalStack evaluation;
    public Filter.FilterType typeId;
    private bool autoDispose;
    private bool active;
    private ulong filteredCount;
    private int priority;

    protected Filter(EvalStack evaluation, Filter.FilterType typeId, bool activate, bool autoDispose, int priority)
    {
      this.typeId = typeId;
      this.active = activate;
      this.evaluation = evaluation;
      this.priority = priority;
      this.autoDispose = autoDispose;
      this.filteredCount = 0UL;
    }

    internal EvalStack Evaluation
    {
      get
      {
        return this.evaluation;
      }
    }

    internal ulong FilteredCount
    {
      get
      {
        return this.filteredCount;
      }
      set
      {
        this.filteredCount = value;
      }
    }

    internal Filter.FilterType TypeId
    {
      get
      {
        return this.typeId;
      }
    }

    internal int Priority
    {
      get
      {
        return this.priority;
      }
    }

    internal bool Active
    {
      get
      {
        return this.active;
      }
    }

    internal virtual string Expression
    {
      get
      {
        if (this.evaluation != null)
          return new string(this.evaluation.Expression);
        return (string) null;
      }
    }

    internal Row.Column FirstColumn
    {
      get
      {
        if (this.evaluation == null)
          return (Row.Column) null;
        return this.evaluation.FirstColumn;
      }
    }

    internal bool GetValidRowStatus(Row row)
    {
      return this.OnGetValidRowStatus(row);
    }

    internal void SetRowStatus(Row row, bool valid)
    {
      this.OnSetRowStatus(row, valid);
    }

    internal bool Activate(bool update)
    {
      return this.OnActivate(update);
    }

    internal void Deactivate()
    {
      this.OnDeactivate();
    }

    protected virtual bool OnGetValidRowStatus(Row row)
    {
      this.evaluation.Exec(row);
      return this.evaluation.TrueBooleanValue;
    }

    protected virtual void OnSetRowStatus(Row row, bool valid)
    {
    }

    protected virtual bool OnActivate(bool update)
    {
      this.active = true;
      return this.Active;
    }

    protected virtual void OnDeactivate()
    {
      this.active = false;
    }

    internal enum FilterType
    {
      Ordinary,
      Optimized,
      DefaultValueInsertGenerator,
      DefaultValueUpdateGenerator,
      Identity,
      ReadOnly,
      ConstraintAppend,
      ConstraintUpdate,
      ConstraintDelete,
      None,
    }
  }
}
