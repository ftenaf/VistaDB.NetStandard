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
    private const int CacheCapacity = 1;
    private VistaDBDataRowCache cache;
    private EditableRow currentRow;
    private bool exclusive;
    private bool readOnly;
    private string tableName;
    private IVistaDBDatabase db;
    private List<IVistaDBColumnAttributes> extendedColumns;

    private VistaDBDataTable(IVistaDBDatabase db, string tblName, bool exclusive, bool readOnly)
    {
      this.tableName = tblName;
      this.exclusive = exclusive;
      this.readOnly = readOnly;
      this.extendedColumns = new List<IVistaDBColumnAttributes>();
      this.db = db;
    }

    public VistaDBDataTable(IVistaDBDatabase db, string tableName)
      : this(db, tableName, true, false)
    {
      this.cache = new VistaDBDataRowCache(db, tableName, this.exclusive, this.readOnly);
    }

    public VistaDBDataTable(IVistaDBDatabase db, string tableName, bool exclusive, bool readOnly, string indexName, int cacheSize, bool optimisticLocking)
      : this(db, tableName, exclusive, readOnly)
    {
      this.cache = new VistaDBDataRowCache(db, tableName, exclusive, readOnly, cacheSize, indexName, optimisticLocking);
    }

    public event ListChangedEventHandler ListChanged;

    public void Close()
    {
      this.cache.CloseTable();
    }

    public void SetFilter(string expression, bool optimize)
    {
      this.cache.SetFilter(expression, optimize);
      if (this.ListChanged == null)
        return;
      this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, 0));
    }

    public bool Find(string keyExpr, string idxName, bool partMach, bool softPos)
    {
      if (!this.cache.Find(keyExpr, idxName, partMach, softPos))
        return false;
      this.RefreshList();
      return true;
    }

    public void ClearFilter()
    {
      this.cache.SetFilter((string) null, true);
      this.RefreshList();
    }

    public void SetScope(string lowExpr, string highExpr)
    {
      this.cache.SetScope(lowExpr, highExpr);
      this.RefreshList();
    }

    public void ClearScope()
    {
      this.cache.ResetScope();
      this.RefreshList();
    }

    public bool SetActiveIndex(string indexName, int selectedRow)
    {
      try
      {
        long num = this.cache.ChangeActiveIndex((long) selectedRow, indexName);
        if (this.ListChanged == null)
          return false;
        if (num > -1L && num < this.cache.TableRowCount)
          this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemMoved, selectedRow, (int) num));
        this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, -1));
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
        return this.cache.GetTableActiveIndex();
      }
    }

    public long GetCurrentRowId(long rowPosition)
    {
      if (this.state != TypeOfOperation.Nothing)
        return -1;
      return this.cache.GetCurrentRowID(rowPosition);
    }

    internal TypeOfOperation State
    {
      get
      {
        return this.state;
      }
      set
      {
        this.state = value;
      }
    }

    internal bool CancelInsert()
    {
      if ((this.state & TypeOfOperation.Insert) == TypeOfOperation.Insert)
      {
        this.allowRemove = true;
        this.cache.CancelInsert();
        if (this.ListChanged != null)
          this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemDeleted, this.Count));
      }
      if (this.state == TypeOfOperation.Update)
      {
        this.cache.ResetInsertedRow();
        this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemChanged, (int) this.curentRowNumber));
      }
      this.state = TypeOfOperation.Nothing;
      return true;
    }

    internal bool ChangeRowValues(long rowPos)
    {
      try
      {
        if (this.ListChanged == null)
          return false;
        this.cache.FillInsertedRow(rowPos);
        if (this.cache.CheckRowCount() != 0)
          this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, 0));
        if (rowPos < (long) this.Count)
          this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemChanged, (int) rowPos));
        return true;
      }
      catch (Exception ex)
      {
        return false;
      }
    }

    internal IVistaDBRow GetDataRow(int index)
    {
      return this.cache.GetDataRow(index);
    }

    internal void SetDataToColumn(int keyIndex, int colIndex, object value)
    {
      this.cache.SetDataToColumn(keyIndex, colIndex, value);
    }

    internal int SynchronizeTableData(int index)
    {
      return this.cache.SynchronizeTableData(index, this.state);
    }

    internal void PostInsert()
    {
      if (this.ListChanged == null)
        return;
      if (this.state == TypeOfOperation.Update)
        this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemChanged, (int) this.curentRowNumber));
      else
        this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemAdded, (int) this.curentRowNumber));
      this.cache.IsInserting = false;
      this.allowRemove = true;
    }

    internal void RefreshList()
    {
      if (this.ListChanged == null)
        return;
      this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, 0));
    }

    void IBindingList.AddIndex(PropertyDescriptor property)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    object IBindingList.AddNew()
    {
      try
      {
        if (this.state == TypeOfOperation.Insert)
          return (object) this.currentRow;
        this.state = TypeOfOperation.Insert;
        if (this.ListChanged == null)
          return (object) null;
        this.cache.InsertRow();
        this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemAdded, this.Count - 1));
        this.allowRemove = false;
        return (object) new EditableRow(this, this.Count - 1);
      }
      catch (VistaDBDataTableException ex)
      {
        this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, 0));
      }
      catch (Exception ex)
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
        return this.allowNew;
      }
    }

    bool IBindingList.AllowRemove
    {
      get
      {
        return this.allowRemove;
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
        if (this.ActiveIndex != "")
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
      if (this.ListChanged != null)
        this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemAdded, this.Count - 1));
      return 0;
    }

    void IList.Clear()
    {
      this.allowNew = false;
      this.allowRemove = false;
      this.cache.CloseTable();
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
      if (this.cache.IsInserting && index == this.Count || (index < 0 || index >= this.Count))
        return;
      this.state = TypeOfOperation.Delete;
      try
      {
        this.cache.DeleteRow((long) index);
        if (this.ListChanged == null)
          return;
        this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.ItemDeleted, index));
      }
      catch (VistaDBException ex)
      {
        if (this.ListChanged != null)
          this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, 0));
        throw;
      }
      catch (Exception ex)
      {
        this.cache.Clear();
        if (this.ListChanged == null)
          return;
        this.ListChanged((object) this, new ListChangedEventArgs(ListChangedType.Reset, 0));
      }
      finally
      {
        this.state = TypeOfOperation.Nothing;
      }
    }

    object IList.this[int index]
    {
      get
      {
        this.currentRow = new EditableRow(this, index);
        this.curentRowNumber = (long) index;
        return (object) this.currentRow;
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
        return (int) this.cache.TableRowCount;
      }
    }

    public IVistaDBColumnAttributes[] GetExtendedNames
    {
      get
      {
        if (this.extendedColumns.Count == 0)
          return (IVistaDBColumnAttributes[]) null;
        IVistaDBColumnAttributes[] columnAttributesArray = new IVistaDBColumnAttributes[this.extendedColumns.Count];
        for (int index = 0; index < columnAttributesArray.Length; ++index)
          columnAttributesArray[index] = this.extendedColumns[index];
        return columnAttributesArray;
      }
    }

    public bool Exclusive
    {
      get
      {
        return this.exclusive;
      }
    }

    public IVistaDBTableSchema TableSchema
    {
      get
      {
        return this.db.TableSchema(this.tableName);
      }
    }

    public bool OptimisticLock
    {
      get
      {
        return this.cache.OptimisticLock;
      }
      set
      {
        this.cache.OptimisticLock = value;
      }
    }

    public bool ReadOnly
    {
      get
      {
        return this.readOnly;
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
        IVistaDBTableSchema vistaDbTableSchema = this.db.TableSchema(this.tableName);
        List<string> stringList = new List<string>(vistaDbTableSchema.Identities.Count);
        foreach (IVistaDBIdentityInformation identityInformation in (IEnumerable<IVistaDBIdentityInformation>) vistaDbTableSchema.Identities.Values)
          stringList.Add(identityInformation.ColumnName);
        this.extendedColumns.Clear();
        foreach (IVistaDBColumnAttributes columnAttributes in (IEnumerable<IVistaDBColumnAttributes>) vistaDbTableSchema)
        {
          if (columnAttributes.ExtendedType || columnAttributes.Type == VistaDBType.VarBinary)
            this.extendedColumns.Add(columnAttributes);
          descriptorCollection.Add((PropertyDescriptor) new VistaDB.Extra.Internal.DataTablePropertyDescriptor(columnAttributes.Name, columnAttributes.SystemType, columnAttributes.RowIndex, columnAttributes.Type, stringList.Contains(columnAttributes.Name)));
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
