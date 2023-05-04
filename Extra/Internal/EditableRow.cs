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
        return parent.GetDataRow(index);
      }
    }

    internal int Index
    {
      get
      {
        return index;
      }
      set
      {
        index = value;
      }
    }

    internal EditableRow(VistaDBDataTable parent, int index)
    {
      this.parent = parent;
      this.index = index;
    }

    internal void SetDataToColumn(int index, object value)
    {
      if (parent.State == TypeOfOperation.Nothing)
        parent.State = TypeOfOperation.Update;
      if (parent.State == TypeOfOperation.Insert)
        parent.State = TypeOfOperation.Insert | TypeOfOperation.Update;
      parent.SetDataToColumn(this.index, index, value);
    }

    void IEditableObject.BeginEdit()
    {
      parent.ChangeRowValues(index);
    }

    void IEditableObject.CancelEdit()
    {
      parent.CancelInsert();
    }

    void IEditableObject.EndEdit()
    {
      try
      {
        switch (parent.SynchronizeTableData(index))
        {
          case 1:
          case 2:
          case 3:
            parent.PostInsert();
            parent.RefreshList();
            parent.State = TypeOfOperation.Nothing;
            break;
        }
      }
      catch (VistaDBDataTableException ex)
      {
        long errorId = ex.ErrorId;
        switch (errorId)
        {
          case 2019:
          case 2020:
          case 2021:
            ((IEditableObject) this).CancelEdit();
            break;
          case 2022:
            parent.RefreshList();
            break;
        }
        if (errorId != 2020L)
          throw ex;
      }
    }
  }
}
