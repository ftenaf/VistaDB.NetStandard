using System.ComponentModel;
using VistaDB.DDA;
using VistaDB.Diagnostic;

namespace VistaDB.Extra.Internal
{
  internal class EditableRow : IEditableObject
  {
    private VistaDBDataTable parent;
    private int index;

    internal IVistaDBRow Row
    {
      get
      {
        return this.parent.GetDataRow(this.index);
      }
    }

    internal int Index
    {
      get
      {
        return this.index;
      }
      set
      {
        this.index = value;
      }
    }

    internal EditableRow(VistaDBDataTable parent, int index)
    {
      this.parent = parent;
      this.index = index;
    }

    internal void SetDataToColumn(int index, object value)
    {
      if (this.parent.State == TypeOfOperation.Nothing)
        this.parent.State = TypeOfOperation.Update;
      if (this.parent.State == TypeOfOperation.Insert)
        this.parent.State = TypeOfOperation.Insert | TypeOfOperation.Update;
      this.parent.SetDataToColumn(this.index, index, value);
    }

    void IEditableObject.BeginEdit()
    {
      this.parent.ChangeRowValues((long) this.index);
    }

    void IEditableObject.CancelEdit()
    {
      this.parent.CancelInsert();
    }

    void IEditableObject.EndEdit()
    {
      try
      {
        switch (this.parent.SynchronizeTableData(this.index))
        {
          case 1:
          case 2:
          case 3:
            this.parent.PostInsert();
            this.parent.RefreshList();
            this.parent.State = TypeOfOperation.Nothing;
            break;
        }
      }
      catch (VistaDBDataTableException ex)
      {
        long errorId = (long) ex.ErrorId;
        switch (errorId)
        {
          case 2019:
          case 2020:
          case 2021:
            ((IEditableObject) this).CancelEdit();
            break;
          case 2022:
            this.parent.RefreshList();
            break;
        }
        if (errorId != 2020L)
          throw ex;
      }
    }
  }
}
