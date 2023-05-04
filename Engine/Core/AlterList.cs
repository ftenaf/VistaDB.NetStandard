using System;
using System.Collections.Generic;
using VistaDB.DDA;
using VistaDB.Diagnostic;

namespace VistaDB.Engine.Core
{
  internal class AlterList : Dictionary<string, AlterList.AlterInformation>
  {
    private List<Row.Column> newColumns = new List<Row.Column>();
    private bool primaryKeyAffected;
    private bool foreignKeyAffected;
    private bool hardChanges;
    private Table.TableSchema oldSchema;
    private Table.TableSchema newSchema;
    private Table.TableSchema.IndexCollection droppedIndexes;
    private Table.TableSchema.IndexCollection updatedIndexes;
    private Table.TableSchema.IndexCollection persistentIndexes;
    private Table.TableSchema.IndexCollection newIndexes;
    private Table.TableSchema.DefaultValueCollection droppedDefaults;
    private Table.TableSchema.DefaultValueCollection updatedDefaults;
    private Table.TableSchema.DefaultValueCollection persistentDefaults;
    private Table.TableSchema.DefaultValueCollection newDefaults;
    private Table.TableSchema.IdentityCollection droppedIdentities;
    private Table.TableSchema.IdentityCollection updatedIdentities;
    private Table.TableSchema.IdentityCollection persistentIdentities;
    private Table.TableSchema.IdentityCollection newIdentities;
    private Table.TableSchema.ConstraintCollection droppedConstraints;
    private Table.TableSchema.ConstraintCollection updatedConstraints;
    private Table.TableSchema.ConstraintCollection persistentConstraints;
    private Table.TableSchema.ConstraintCollection newConstraints;

    internal AlterList(Table.TableSchema oldSchema, Table.TableSchema newSchema)
      : base((IEqualityComparer<string>) StringComparer.OrdinalIgnoreCase)
    {
      this.oldSchema = oldSchema;
      this.newSchema = newSchema;
    }

    internal new AlterInformation this[string name]
    {
      get
      {
        if (!ContainsKey(name))
          return (AlterInformation) null;
        return base[name];
      }
    }

    internal Table.TableSchema OldSchema
    {
      get
      {
        return oldSchema;
      }
    }

    internal Table.TableSchema NewSchema
    {
      get
      {
        return newSchema;
      }
    }

    internal Table.TableSchema.IndexCollection DroppedIndexes
    {
      get
      {
        return droppedIndexes;
      }
    }

    internal Table.TableSchema.IndexCollection UpdatedIndexes
    {
      get
      {
        return updatedIndexes;
      }
    }

    internal Table.TableSchema.IndexCollection PersistentIndexes
    {
      get
      {
        return persistentIndexes;
      }
    }

    internal Table.TableSchema.IndexCollection NewIndexes
    {
      get
      {
        return newIndexes;
      }
    }

    internal Table.TableSchema.DefaultValueCollection DroppedDefaults
    {
      get
      {
        return droppedDefaults;
      }
    }

    internal Table.TableSchema.DefaultValueCollection UpdatedDefaults
    {
      get
      {
        return updatedDefaults;
      }
    }

    internal Table.TableSchema.DefaultValueCollection PersistentDefaults
    {
      get
      {
        return persistentDefaults;
      }
    }

    internal Table.TableSchema.DefaultValueCollection NewDefaults
    {
      get
      {
        return newDefaults;
      }
    }

    internal Table.TableSchema.IdentityCollection DroppedIdentities
    {
      get
      {
        return droppedIdentities;
      }
    }

    internal Table.TableSchema.IdentityCollection UpdatedIdentities
    {
      get
      {
        return updatedIdentities;
      }
    }

    internal Table.TableSchema.IdentityCollection PersistentIdentities
    {
      get
      {
        return persistentIdentities;
      }
    }

    internal Table.TableSchema.IdentityCollection NewIdentities
    {
      get
      {
        return newIdentities;
      }
    }

    internal Table.TableSchema.ConstraintCollection DroppedConstraints
    {
      get
      {
        return droppedConstraints;
      }
    }

    internal Table.TableSchema.ConstraintCollection UpdatedConstraints
    {
      get
      {
        return updatedConstraints;
      }
    }

    internal Table.TableSchema.ConstraintCollection PersistentConstraints
    {
      get
      {
        return persistentConstraints;
      }
    }

    internal Table.TableSchema.ConstraintCollection NewConstraints
    {
      get
      {
        return newConstraints;
      }
    }

    internal bool IsPrimaryKeyAffected
    {
      get
      {
        return primaryKeyAffected;
      }
    }

    internal bool IsForeignKeyAffected
    {
      get
      {
        return foreignKeyAffected;
      }
    }

    internal bool AnalyzeChanges()
    {
      bool flag = AnalyzeColumnsChanges();
      if (!AnalyzeMetaObjects())
        return flag;
      return true;
    }

    internal bool TakeNewTableDecision(bool forceAlter, Database db)
    {
      if (primaryKeyAffected)
      {
        string relation = (string) null;
        if (db.IsReferencedPK(oldSchema.Name, ref relation))
          throw new VistaDBException(321, oldSchema.Name);
      }
      if (!foreignKeyAffected)
        return hardChanges;
      if (forceAlter)
        return true;
      throw new VistaDBException(321, oldSchema.Name);
    }

    internal IVistaDBColumnAttributes GetNewColumn(string oldColumnName)
    {
      if (!ContainsKey(oldColumnName))
        return (IVistaDBColumnAttributes) null;
      return this[oldColumnName].NewColumn;
    }

    internal IVistaDBColumnAttributes GetOldColumn(string oldColumnName)
    {
      if (!ContainsKey(oldColumnName))
        return (IVistaDBColumnAttributes) null;
      return this[oldColumnName].OldColumn;
    }

    internal void FillTemporarySchema(IVistaDBTableSchema temporary)
    {
      ((Table.TableSchema) temporary).TemporarySchema = true;
      foreach (IVistaDBColumnAttributes columnAttributes in (List<Row.Column>) newSchema)
      {
        ((Table.TableSchema) temporary).AddColumn(columnAttributes.Name, columnAttributes.Type, columnAttributes.MaxLength, columnAttributes.CodePage, ((Row.Column) columnAttributes).IsSync);
        temporary.DefineColumnAttributes(columnAttributes.Name, columnAttributes.AllowNull, columnAttributes.ReadOnly, columnAttributes.Encrypted, columnAttributes.Packed, columnAttributes.Caption, columnAttributes.Description);
      }
      foreach (Table.TableSchema.ConstraintCollection.ConstraintInformation constraintInformation in newSchema.Constraints.Values)
      {
        if (!droppedConstraints.ContainsKey(constraintInformation.Name))
          ((Dictionary<string, IVistaDBConstraintInformation>) temporary.Constraints).Add(constraintInformation.Name, (IVistaDBConstraintInformation) constraintInformation);
      }
      foreach (IVistaDBDefaultValueInformation valueInformation in newSchema.Defaults.Values)
      {
        if (!droppedDefaults.ContainsKey(valueInformation.Name))
          ((Dictionary<string, IVistaDBDefaultValueInformation>) temporary.DefaultValues).Add(valueInformation.Name, valueInformation);
      }
      foreach (IVistaDBIdentityInformation identityInformation in newSchema.Identities.Values)
      {
        if (!droppedIdentities.ContainsKey(identityInformation.Name))
          ((Dictionary<string, IVistaDBIdentityInformation>) temporary.Identities).Add(identityInformation.Name, identityInformation);
      }
      foreach (IVistaDBIndexInformation indexInformation in newSchema.Indexes.Values)
      {
        if (droppedIndexes.ContainsKey(indexInformation.Name))
          ((Dictionary<string, IVistaDBIndexInformation>) temporary.Indexes).Remove(indexInformation.Name);
        ((Dictionary<string, IVistaDBIndexInformation>) temporary.Indexes).Add(indexInformation.Name, indexInformation);
      }
    }

    private bool AnalyzeColumnsChanges()
    {
      Dictionary<int, string> droppedColumns = newSchema.DroppedColumns;
      foreach (string index in droppedColumns.Values)
      {
        Row.Column column = oldSchema[index];
        Add(column.Name, new AlterInformation((IVistaDBColumnAttributes) column, (IVistaDBColumnAttributes) null));
      }
      Dictionary<int, string> renamedColumns = newSchema.RenamedColumns;
      foreach (Row.Column column1 in (List<Row.Column>) newSchema)
      {
        string key;
        if (renamedColumns.TryGetValue(column1.UniqueID, out key))
        {
          Row.Column column2 = oldSchema[key];
          Add(key, new AlterInformation((IVistaDBColumnAttributes) column2, (IVistaDBColumnAttributes) column1));
        }
        else if (ContainsKey(column1.Name))
        {
          newColumns.Add(column1);
        }
        else
        {
          Row.Column column2 = oldSchema[column1.Name];
          if (column2 == (Row.Column) null)
            newColumns.Add(column1);
          else
            Add(column2.Name, new AlterInformation((IVistaDBColumnAttributes) column2, (IVistaDBColumnAttributes) column1));
        }
      }
      primaryKeyAffected = false;
      foreignKeyAffected = false;
      hardChanges = newColumns.Count != 0 || droppedColumns.Count != 0;
      bool flag = false;
      foreach (AlterInformation alterInformation in Values)
      {
        alterInformation.AnalyzePropertiesChanges((IVistaDBIndexCollection) oldSchema.Indexes);
        hardChanges = hardChanges || alterInformation.HardChanges;
        primaryKeyAffected = primaryKeyAffected || alterInformation.PrimaryKeyAffected;
        foreignKeyAffected = foreignKeyAffected || alterInformation.ForeignKeyAffected;
        flag = flag || alterInformation.PersistentChanges;
      }
      foreach (Row.Column newColumn in newColumns)
      {
        IVistaDBDefaultValueInformation toDefaults;
        if (newSchema.Defaults.TryGetValue(newColumn.Name, out toDefaults))
          Add("New_" + newColumn.Name, new AlterInformation(toDefaults, (IVistaDBColumnAttributes) newColumn));
      }
      if (!hardChanges && !primaryKeyAffected && !foreignKeyAffected)
        return flag;
      return true;
    }

    private bool DecideAboutForeignKeyChanges()
    {
      return false;
    }

    private void ProcessIndexChanges(IVistaDBIndexCollection oldList, IVistaDBIndexCollection newList, out Table.TableSchema.IndexCollection droppedItems, out Table.TableSchema.IndexCollection updatedItems, out Table.TableSchema.IndexCollection persistentItems, out Table.TableSchema.IndexCollection newItems)
    {
      droppedItems = new Table.TableSchema.IndexCollection();
      updatedItems = new Table.TableSchema.IndexCollection();
      persistentItems = new Table.TableSchema.IndexCollection();
      newItems = new Table.TableSchema.IndexCollection();
      foreach (Table.TableSchema.IndexCollection.IndexInformation indexInformation1 in (IEnumerable<IVistaDBIndexInformation>) oldList.Values)
      {
        string name = indexInformation1.Name;
        Table.TableSchema.IndexCollection.IndexInformation indexInformation2 = (Table.TableSchema.IndexCollection.IndexInformation) newList[name];
        if (indexInformation2 == null)
          droppedItems.Add(name, (IVistaDBIndexInformation) indexInformation1);
        else if (!indexInformation1.Equals((object) indexInformation2))
          updatedItems.Add(name, (IVistaDBIndexInformation) indexInformation2);
        else
          persistentItems.Add(name, (IVistaDBIndexInformation) indexInformation2);
      }
      foreach (Table.TableSchema.IndexCollection.IndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>) newList.Values)
      {
        if (!oldList.ContainsKey(indexInformation.Name))
          newItems.Add(indexInformation.Name, (IVistaDBIndexInformation) indexInformation);
      }
    }

    private void ProcessConstraintChanges(IVistaDBConstraintCollection oldList, IVistaDBConstraintCollection newList, out Table.TableSchema.ConstraintCollection droppedItems, out Table.TableSchema.ConstraintCollection updatedItems, out Table.TableSchema.ConstraintCollection persistentItems, out Table.TableSchema.ConstraintCollection newItems)
    {
      droppedItems = new Table.TableSchema.ConstraintCollection();
      updatedItems = new Table.TableSchema.ConstraintCollection();
      persistentItems = new Table.TableSchema.ConstraintCollection();
      newItems = new Table.TableSchema.ConstraintCollection();
      foreach (Table.TableSchema.ConstraintCollection.ConstraintInformation constraintInformation1 in (IEnumerable<IVistaDBConstraintInformation>) oldList.Values)
      {
        string name = constraintInformation1.Name;
        Table.TableSchema.ConstraintCollection.ConstraintInformation constraintInformation2 = (Table.TableSchema.ConstraintCollection.ConstraintInformation) newList[name];
        if (constraintInformation2 == null)
          droppedItems.Add(name, (IVistaDBConstraintInformation) constraintInformation1);
        else if (!constraintInformation1.Equals((object) constraintInformation2))
          updatedItems.Add(name, (IVistaDBConstraintInformation) constraintInformation2);
        else
          persistentItems.Add(name, (IVistaDBConstraintInformation) constraintInformation2);
      }
      foreach (Table.TableSchema.ConstraintCollection.ConstraintInformation constraintInformation in (IEnumerable<IVistaDBConstraintInformation>) newList.Values)
      {
        if (!oldList.ContainsKey(constraintInformation.Name))
          newItems.Add(constraintInformation.Name, (IVistaDBConstraintInformation) constraintInformation);
      }
    }

    private void ProcessDefaultsChanges(IVistaDBDefaultValueCollection oldList, IVistaDBDefaultValueCollection newList, out Table.TableSchema.DefaultValueCollection droppedItems, out Table.TableSchema.DefaultValueCollection updatedItems, out Table.TableSchema.DefaultValueCollection persistentItems, out Table.TableSchema.DefaultValueCollection newItems)
    {
      droppedItems = new Table.TableSchema.DefaultValueCollection();
      updatedItems = new Table.TableSchema.DefaultValueCollection();
      persistentItems = new Table.TableSchema.DefaultValueCollection();
      newItems = new Table.TableSchema.DefaultValueCollection();
      foreach (Table.TableSchema.DefaultValueCollection.DefaultValueInformation valueInformation1 in (IEnumerable<IVistaDBDefaultValueInformation>) oldList.Values)
      {
        string name1 = valueInformation1.Name;
                AlterInformation alterInformation = this[name1];
        if (alterInformation.NewColumn == null)
        {
          droppedItems.Add(name1, (IVistaDBDefaultValueInformation) valueInformation1);
        }
        else
        {
          string name2 = alterInformation.NewColumn.Name;
          Table.TableSchema.DefaultValueCollection.DefaultValueInformation valueInformation2 = (Table.TableSchema.DefaultValueCollection.DefaultValueInformation) newList[name2];
          if (valueInformation2 == null)
            droppedItems.Add(name1, (IVistaDBDefaultValueInformation) valueInformation1);
          else if (!valueInformation1.Equals((object) valueInformation2))
            updatedItems.Add(name1, (IVistaDBDefaultValueInformation) valueInformation2);
          else
            persistentItems.Add(name1, (IVistaDBDefaultValueInformation) valueInformation2);
        }
      }
      foreach (Table.TableSchema.DefaultValueCollection.DefaultValueInformation valueInformation in (IEnumerable<IVistaDBDefaultValueInformation>) newList.Values)
      {
        if (!updatedItems.ContainsValue((IVistaDBDefaultValueInformation) valueInformation) && !persistentItems.ContainsValue((IVistaDBDefaultValueInformation) valueInformation))
          newItems.Add(valueInformation.Name, (IVistaDBDefaultValueInformation) valueInformation);
      }
    }

    private void ProcessIdentitiesChanges(IVistaDBIdentityCollection oldList, IVistaDBIdentityCollection newList, out Table.TableSchema.IdentityCollection droppedItems, out Table.TableSchema.IdentityCollection updatedItems, out Table.TableSchema.IdentityCollection persistentItems, out Table.TableSchema.IdentityCollection newItems)
    {
      droppedItems = new Table.TableSchema.IdentityCollection();
      updatedItems = new Table.TableSchema.IdentityCollection();
      persistentItems = new Table.TableSchema.IdentityCollection();
      newItems = new Table.TableSchema.IdentityCollection();
      foreach (Table.TableSchema.IdentityCollection.IdentityInformation originalIdentity in (IEnumerable<IVistaDBIdentityInformation>) oldList.Values)
      {
        string name1 = originalIdentity.Name;
                AlterInformation alterInformation = this[name1];
        if (alterInformation.NewColumn == null)
        {
          droppedItems.Add(name1, (IVistaDBIdentityInformation) originalIdentity);
        }
        else
        {
          string name2 = alterInformation.NewColumn.Name;
          Table.TableSchema.IdentityCollection.IdentityInformation identityInformation = (Table.TableSchema.IdentityCollection.IdentityInformation) newList[name2];
          if (identityInformation == null)
            droppedItems.Add(name1, (IVistaDBIdentityInformation) originalIdentity);
          else if (!originalIdentity.Equals((object) identityInformation))
          {
            if (!ReferenceEquals((object) originalIdentity, (object) identityInformation))
              identityInformation.CopySeedValue(originalIdentity);
            updatedItems.Add(name1, (IVistaDBIdentityInformation) identityInformation);
          }
          else
            persistentItems.Add(name1, (IVistaDBIdentityInformation) identityInformation);
        }
      }
      foreach (Table.TableSchema.IdentityCollection.IdentityInformation identityInformation in (IEnumerable<IVistaDBIdentityInformation>) newList.Values)
      {
        if (!updatedItems.ContainsValue((IVistaDBIdentityInformation) identityInformation) && !persistentItems.ContainsValue((IVistaDBIdentityInformation) identityInformation))
          newItems.Add(identityInformation.Name, (IVistaDBIdentityInformation) identityInformation);
      }
    }

    private bool AnalyzeMetaObjects()
    {
      bool flag1 = false;
      ProcessDefaultsChanges((IVistaDBDefaultValueCollection) oldSchema.Defaults, (IVistaDBDefaultValueCollection) newSchema.Defaults, out droppedDefaults, out updatedDefaults, out persistentDefaults, out newDefaults);
      bool flag2 = flag1 || updatedDefaults.Count > 0 || newDefaults.Count > 0;
      ProcessIdentitiesChanges((IVistaDBIdentityCollection) oldSchema.Identities, (IVistaDBIdentityCollection) newSchema.Identities, out droppedIdentities, out updatedIdentities, out persistentIdentities, out newIdentities);
      bool flag3 = flag2 || updatedIdentities.Count > 0 || newIdentities.Count > 0;
      ProcessIndexChanges((IVistaDBIndexCollection) oldSchema.Indexes, (IVistaDBIndexCollection) newSchema.Indexes, out droppedIndexes, out updatedIndexes, out persistentIndexes, out newIndexes);
      bool flag4 = flag3 || updatedIndexes.Count > 0 || newIndexes.Count > 0;
      ProcessConstraintChanges((IVistaDBConstraintCollection) oldSchema.Constraints, (IVistaDBConstraintCollection) newSchema.Constraints, out droppedConstraints, out updatedConstraints, out persistentConstraints, out newConstraints);
      return DecideAboutForeignKeyChanges() || (flag4 || updatedConstraints.Count > 0 || newConstraints.Count > 0);
    }

    internal class AlterInformation
    {
      private IVistaDBColumnAttributes oldColumn;
      private IVistaDBColumnAttributes newColumn;
      private IVistaDBDefaultValueInformation newDefaults;
      private IVistaDBColumnAttributesDifference difference;
      private bool deleted;
      private bool participatePrimary;
      private bool participateForeignKey;
      private bool hardChanges;
      private bool persistentChanges;

      internal AlterInformation(IVistaDBColumnAttributes fromColumn, IVistaDBColumnAttributes toColumn)
      {
        oldColumn = fromColumn;
        newColumn = toColumn;
        deleted = toColumn == null;
        difference = deleted ? (IVistaDBColumnAttributesDifference) null : fromColumn.Compare(toColumn);
      }

      internal AlterInformation(IVistaDBDefaultValueInformation toDefaults, IVistaDBColumnAttributes toColumn)
      {
        newDefaults = toDefaults;
        newColumn = toColumn;
        deleted = false;
        difference = (IVistaDBColumnAttributesDifference) null;
      }

      internal void AnalyzePropertiesChanges(IVistaDBIndexCollection indexes)
      {
        foreach (IVistaDBIndexInformation indexInformation in (IEnumerable<IVistaDBIndexInformation>) indexes.Values)
        {
          IVistaDBKeyColumn[] keyStructure = indexInformation.KeyStructure;
          if (indexInformation.Primary || indexInformation.FKConstraint)
          {
            foreach (IVistaDBKeyColumn vistaDbKeyColumn in keyStructure)
            {
              if (oldColumn.RowIndex == vistaDbKeyColumn.RowIndex)
              {
                participatePrimary = indexInformation.Primary;
                participateForeignKey = indexInformation.FKConstraint;
                break;
              }
            }
          }
        }
        hardChanges = deleted || difference.IsMaxLengthDiffers || (difference.IsPackedDiffers || difference.IsEncryptedDiffers) || (difference.IsTypeDiffers || difference.IsCodePageDiffers) || difference.IsOrderDiffers;
        persistentChanges = !hardChanges && (difference.IsRenamed || difference.IsReadOnlyDiffers || (difference.IsNullDiffers || difference.IsCaptionDiffers) || difference.IsDescriptionDiffers);
      }

      internal IVistaDBDefaultValueInformation NewDefaults
      {
        get
        {
          return newDefaults;
        }
      }

      internal IVistaDBColumnAttributes OldColumn
      {
        get
        {
          return oldColumn;
        }
      }

      internal IVistaDBColumnAttributes NewColumn
      {
        get
        {
          return newColumn;
        }
      }

      internal bool HardChanges
      {
        get
        {
          return hardChanges;
        }
      }

      internal bool PersistentChanges
      {
        get
        {
          return persistentChanges;
        }
      }

      internal bool Renamed
      {
        get
        {
          return difference.IsRenamed;
        }
      }

      internal bool ForeignKeyAffected
      {
        get
        {
          if (!participateForeignKey)
            return false;
          if (!deleted && !difference.IsTypeDiffers)
            return difference.IsMaxLengthDiffers;
          return true;
        }
      }

      internal bool PrimaryKeyAffected
      {
        get
        {
          if (!participatePrimary)
            return false;
          if (!deleted && !difference.IsTypeDiffers)
            return difference.IsMaxLengthDiffers;
          return true;
        }
      }
    }
  }
}
