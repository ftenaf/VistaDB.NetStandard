using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Extra.Internal;

namespace VistaDB.Extra
{
  public class VistaDBDataTable : IBindingList, IList, ICollection, IEnumerable, ITypedList, IEditableObject, ISupportInitialize
  {
    private long curentRowNumber = -1;
    private TypeOfOperation state = TypeOfOperation.Nothing;
    private bool allowRemove = true;
    private bool allowNew = true;
        private VistaDBDataRowCache cache;
    private EditableRow currentRow;
    private bool exclusive;
    private bool readOnly;
    private string tableName;
    private IVistaDBDatabase db;
    private List<IVistaDBColumnAttributes> extendedColumns;

    private VistaDBDataTable(IVistaDBDatabase db, string tblName, bool exclusive, bool readOnly)
    {
      tableName = tblName;
      this.exclusive = exclusive;
      this.readOnly = readOnly;
      extendedColumns = new List<IVistaDBColumnAttributes>();
      this.db = db;
    }

    public VistaDBDataTable(IVistaDBDatabase db, string tableName)
      : this(db, tableName, true, false)
    {
      cache = new VistaDBDataRowCache(db, tableName, exclusive, readOnly);
    }

    public VistaDBDataTable(IVistaDBDatabase db, string tableName, bool exclusive, bool readOnly, string indexName, int cacheSize, bool optimisticLocking)
      : this(db, tableName, exclusive, readOnly)
    {
      cache = new VistaDBDataRowCache(db, tableName, exclusive, readOnly, cacheSize, indexName, optimisticLocking);
    }

    public event ListChangedEventHandler ListChanged;

    public void Close()
    {
      cache.CloseTable();
    }

    public void SetFilter(string expression, bool optimize)
    {
      cache.SetFilter(expression, optimize);
      if (ListChanged == null)
        return;
      ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, 0));
    }

    public bool Find(string keyExpr, string idxName, bool partMach, bool softPos)
    {
      if (!cache.Find(keyExpr, idxName, partMach, softPos))
        return false;
      RefreshList();
      return true;
    }

    public void ClearFilter()
    {
      cache.SetFilter((string) null, true);
      RefreshList();
    }

    public void SetScope(string lowExpr, string highExpr)
    {
      cache.SetScope(lowExpr, highExpr);
      RefreshList();
    }

    public void ClearScope()
    {
      cache.ResetScope();
      RefreshList();
    }

    public bool SetActiveIndex(string indexName, int selectedRow)
    {
      try
      {
        long num = cache.ChangeActiveIndex((long) selectedRow, indexName);
        if (ListChanged == null)
          return false;
        if (num > -1L && num < cache.TableRowCount)
          ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemMoved, selectedRow, (int) num));
        ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, -1));
      }
      catch (VistaDBDataTableException ex)
      {
        DataException dataException = new DataException(ex.Message, ex.InnerException);
        return false;
      }
      return true;
    }

    public string ActiveIndex
    {
      get
      {
        return cache.GetTableActiveIndex();
      }
    }

    public long GetCurrentRowId(long rowPosition)
    {
      if (state != TypeOfOperation.Nothing)
        return -1;
      return cache.GetCurrentRowID(rowPosition);
    }

    internal TypeOfOperation State
    {
      get
      {
        return state;
      }
      set
      {
        state = value;
      }
    }

    internal bool CancelInsert()
    {
      if ((state & TypeOfOperation.Insert) == TypeOfOperation.Insert)
      {
        allowRemove = true;
        cache.CancelInsert();
        if (ListChanged != null)
          ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemDeleted, Count));
      }
      if (state == TypeOfOperation.Update)
      {
        cache.ResetInsertedRow();
        ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemChanged, (int) curentRowNumber));
      }
      state = TypeOfOperation.Nothing;
      return true;
    }

    internal bool ChangeRowValues(long rowPos)
    {
      try
      {
        if (ListChanged == null)
          return false;
        cache.FillInsertedRow(rowPos);
        if (cache.CheckRowCount() != 0)
          ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, 0));
        if (rowPos < (long) Count)
          ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemChanged, (int) rowPos));
        return true;
      }
      catch (Exception)
            {
        return false;
      }
    }

    internal IVistaDBRow GetDataRow(int index)
    {
      return cache.GetDataRow(index);
    }

    internal void SetDataToColumn(int keyIndex, int colIndex, object value)
    {
      cache.SetDataToColumn(keyIndex, colIndex, value);
    }

    internal int SynchronizeTableData(int index)
    {
      return cache.SynchronizeTableData(index, state);
    }

    internal void PostInsert()
    {
      if (ListChanged == null)
        return;
      if (state == TypeOfOperation.Update)
        ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemChanged, (int) curentRowNumber));
      else
        ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemAdded, (int) curentRowNumber));
      cache.IsInserting = false;
      allowRemove = true;
    }

    internal void RefreshList()
    {
      if (ListChanged == null)
        return;
      ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, 0));
    }

    void IBindingList.AddIndex(PropertyDescriptor property)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    object IBindingList.AddNew()
    {
      try
      {
        if (state == TypeOfOperation.Insert)
          return (object) currentRow;
        state = TypeOfOperation.Insert;
        if (ListChanged == null)
          return (object) null;
        cache.InsertRow();
        ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemAdded, Count - 1));
        allowRemove = false;
        return (object) new EditableRow(this, Count - 1);
      }
      catch (VistaDBDataTableException)
            {
        ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, 0));
      }
      catch (Exception)
            {
      }
      return (object) null;
    }

    bool IBindingList.AllowEdit
    {
      get
      {
        return true;
      }
    }

    bool IBindingList.AllowNew
    {
      get
      {
        return allowNew;
      }
    }

    bool IBindingList.AllowRemove
    {
      get
      {
        return allowRemove;
      }
    }

    void IBindingList.ApplySort(PropertyDescriptor property, ListSortDirection direction)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    int IBindingList.Find(PropertyDescriptor property, object key)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    bool IBindingList.IsSorted
    {
      get
      {
        if (ActiveIndex != "")
          return ((IBindingList) this).SortProperty != null;
        return false;
      }
    }

    void IBindingList.RemoveIndex(PropertyDescriptor property)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    void IBindingList.RemoveSort()
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    ListSortDirection IBindingList.SortDirection
    {
      get
      {
        throw new NotImplementedException("The method or operation is not implemented.");
      }
    }

    PropertyDescriptor IBindingList.SortProperty
    {
      get
      {
        throw new NotImplementedException("The method or operation is not implemented.");
      }
    }

    bool IBindingList.SupportsChangeNotification
    {
      get
      {
        return true;
      }
    }

    bool IBindingList.SupportsSearching
    {
      get
      {
        return false;
      }
    }

    bool IBindingList.SupportsSorting
    {
      get
      {
        return false;
      }
    }

    int IList.Add(object value)
    {
      if (ListChanged != null)
        ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemAdded, Count - 1));
      return 0;
    }

    void IList.Clear()
    {
      allowNew = false;
      allowRemove = false;
      cache.CloseTable();
    }

    bool IList.Contains(object value)
    {
      throw new Exception("The method or operation is not implemented.");
    }

    int IList.IndexOf(object value)
    {
      throw new Exception("The method or operation is not implemented.");
    }

    void IList.Insert(int index, object value)
    {
      throw new Exception("The method or operation is not implemented.");
    }

    bool IList.IsFixedSize
    {
      get
      {
        throw new Exception("The method or operation is not implemented.");
      }
    }

    bool IList.IsReadOnly
    {
      get
      {
        throw new Exception("The method or operation is not implemented.");
      }
    }

    void IList.Remove(object value)
    {
      throw new Exception("The method or operation is not implemented.");
    }

    void IList.RemoveAt(int index)
    {
      if (cache.IsInserting && index == Count || (index < 0 || index >= Count))
        return;
      state = TypeOfOperation.Delete;
      try
      {
        cache.DeleteRow((long) index);
        if (ListChanged == null)
          return;
        ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemDeleted, index));
      }
      catch (VistaDBException)
            {
        if (ListChanged != null)
          ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, 0));
        throw;
      }
      catch (Exception)
            {
        cache.Clear();
        if (ListChanged == null)
          return;
        ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, 0));
      }
      finally
      {
        state = TypeOfOperation.Nothing;
      }
    }

    object IList.this[int index]
    {
      get
      {
        currentRow = new EditableRow(this, index);
        curentRowNumber = (long) index;
        return (object) currentRow;
      }
      set
      {
      }
    }

    void ICollection.CopyTo(Array array, int index)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public int Count
    {
      get
      {
        return (int) cache.TableRowCount;
      }
    }

    public IVistaDBColumnAttributes[] GetExtendedNames
    {
      get
      {
        if (extendedColumns.Count == 0)
          return (IVistaDBColumnAttributes[]) null;
        IVistaDBColumnAttributes[] columnAttributesArray = new IVistaDBColumnAttributes[extendedColumns.Count];
        for (int index = 0; index < columnAttributesArray.Length; ++index)
          columnAttributesArray[index] = extendedColumns[index];
        return columnAttributesArray;
      }
    }

    public bool Exclusive
    {
      get
      {
        return exclusive;
      }
    }

    public IVistaDBTableSchema TableSchema
    {
      get
      {
        return db.TableSchema(tableName);
      }
    }

    public bool OptimisticLock
    {
      get
      {
        return cache.OptimisticLock;
      }
      set
      {
        cache.OptimisticLock = value;
      }
    }

    public bool ReadOnly
    {
      get
      {
        return readOnly;
      }
    }

    bool ICollection.IsSynchronized
    {
      get
      {
        return false;
      }
    }

    object ICollection.SyncRoot
    {
      get
      {
        return (object) this;
      }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return (IEnumerator) new DataTableEnumerator(this);
    }

    PropertyDescriptorCollection ITypedList.GetItemProperties(PropertyDescriptor[] listAccessors)
    {
      try
      {
        PropertyDescriptorCollection descriptorCollection = new PropertyDescriptorCollection((PropertyDescriptor[]) null);
        IVistaDBTableSchema vistaDbTableSchema = db.TableSchema(tableName);
        List<string> stringList = new List<string>(vistaDbTableSchema.Identities.Count);
        foreach (IVistaDBIdentityInformation identityInformation in (IEnumerable<IVistaDBIdentityInformation>) vistaDbTableSchema.Identities.Values)
          stringList.Add(identityInformation.ColumnName);
        extendedColumns.Clear();
        foreach (IVistaDBColumnAttributes columnAttributes in (IEnumerable<IVistaDBColumnAttributes>) vistaDbTableSchema)
        {
          if (columnAttributes.ExtendedType || columnAttributes.Type == VistaDBType.VarBinary)
            extendedColumns.Add(columnAttributes);
          descriptorCollection.Add((PropertyDescriptor) new DataTablePropertyDescriptor(columnAttributes.Name, columnAttributes.SystemType, columnAttributes.RowIndex, columnAttributes.Type, stringList.Contains(columnAttributes.Name)));
        }
        return descriptorCollection;
      }
      catch (VistaDBException ex)
      {
        throw new VistaDBDataTableException((Exception) ex, 2040);
      }
    }

    string ITypedList.GetListName(PropertyDescriptor[] listAccessors)
    {
      return "VistaDBTable List";
    }

    void IEditableObject.BeginEdit()
    {
      throw new Exception("The method or operation is not implemented.");
    }

    void IEditableObject.CancelEdit()
    {
      throw new Exception("The method or operation is not implemented.");
    }

    void IEditableObject.EndEdit()
    {
      throw new Exception("The method or operation is not implemented.");
    }

    void ISupportInitialize.BeginInit()
    {
    }

    void ISupportInitialize.EndInit()
    {
    }
  }
}
