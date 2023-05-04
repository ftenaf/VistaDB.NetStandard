using VistaDB.Engine.Core.Scripting;

namespace VistaDB.Engine.Core
{
  internal class Filter
  {
    private EvalStack evaluation;
    public FilterType typeId;
    private bool autoDispose;
    private bool active;
    private ulong filteredCount;
    private int priority;

    protected Filter(EvalStack evaluation, FilterType typeId, bool activate, bool autoDispose, int priority)
    {
      this.typeId = typeId;
      active = activate;
      this.evaluation = evaluation;
      this.priority = priority;
      this.autoDispose = autoDispose;
      filteredCount = 0UL;
    }

    internal EvalStack Evaluation
    {
      get
      {
        return evaluation;
      }
    }

    internal ulong FilteredCount
    {
      get
      {
        return filteredCount;
      }
      set
      {
        filteredCount = value;
      }
    }

    internal FilterType TypeId
    {
      get
      {
        return typeId;
      }
    }

    internal int Priority
    {
      get
      {
        return priority;
      }
    }

    internal bool Active
    {
      get
      {
        return active;
      }
    }

    internal virtual string Expression
    {
      get
      {
        if (evaluation != null)
          return new string(evaluation.Expression);
        return (string) null;
      }
    }

    internal Row.Column FirstColumn
    {
      get
      {
        if (evaluation == null)
          return (Row.Column) null;
        return evaluation.FirstColumn;
      }
    }

    internal bool GetValidRowStatus(Row row)
    {
      return OnGetValidRowStatus(row);
    }

    internal void SetRowStatus(Row row, bool valid)
    {
      OnSetRowStatus(row, valid);
    }

    internal bool Activate(bool update)
    {
      return OnActivate(update);
    }

    internal void Deactivate()
    {
      OnDeactivate();
    }

    protected virtual bool OnGetValidRowStatus(Row row)
    {
      evaluation.Exec(row);
      return evaluation.TrueBooleanValue;
    }

    protected virtual void OnSetRowStatus(Row row, bool valid)
    {
    }

    protected virtual bool OnActivate(bool update)
    {
      active = true;
      return Active;
    }

    protected virtual void OnDeactivate()
    {
      active = false;
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
