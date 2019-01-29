using System.Collections.Generic;
using VistaDB.Engine.Core.Scripting;

namespace VistaDB.Engine.Core
{
  internal class Relationships : List<Relationships.Relation>
  {
    private int freezeCounter;

    internal void AddRelation(DataStorage masterStorage, DataStorage slaveStorage, Relationships.Type type, EvalStack linking, bool maxPriority)
    {
      foreach (Relationships.Relation relation in (List<Relationships.Relation>) this)
      {
        if (relation.MasterStorage == masterStorage && relation.SlaveStorage == slaveStorage)
          return;
      }
      Relationships.Relation relation1 = new Relationships.Relation(masterStorage, slaveStorage, type, linking);
      if (maxPriority)
        this.Insert(0, relation1);
      else
        this.Add(relation1);
    }

    internal void RemoveRelation(DataStorage masterStorage, DataStorage slaveStorage)
    {
      for (int index = this.Count - 1; index >= 0; --index)
      {
        Relationships.Relation relation = this[index];
        if (relation.MasterStorage == masterStorage && relation.SlaveStorage == slaveStorage)
        {
          this.RemoveAt(index);
          break;
        }
      }
    }

    internal bool Freeze(bool currentStatus)
    {
      if (++this.freezeCounter > 1)
        return currentStatus;
      bool flag = false;
      foreach (Relationships.Relation relation in (List<Relationships.Relation>) this)
      {
        relation.Active = false;
        flag = flag || relation.Active;
      }
      return flag;
    }

    internal bool Defreeze(bool currentStatus)
    {
      if (--this.freezeCounter > 0)
        return currentStatus;
      this.freezeCounter = 0;
      foreach (Relationships.Relation relation in (List<Relationships.Relation>) this)
        relation.Active = true;
      return true;
    }

    internal bool MaskRelationship(DataStorage masterStorage, DataStorage slaveStorage, bool activate)
    {
      foreach (Relationships.Relation relation in (List<Relationships.Relation>) this)
      {
        if (relation.MasterStorage == masterStorage && relation.SlaveStorage == slaveStorage)
        {
          bool active = relation.Active;
          relation.Active = activate;
          return active;
        }
      }
      return false;
    }

    internal enum Type
    {
      One_To_One,
      OneOrZero_To_One,
      One_To_ZeroOrOne,
      OneOrZero_To_OneOrZero,
      Many_To_OneOrZero,
    }

    internal class Relation
    {
      private DataStorage masterStorage;
      private DataStorage slaveStorage;
      private EvalStack evaluation;
      private Relationships.Type type;
      private bool active;

      internal Relation(DataStorage masterStorage, DataStorage slaveStorage, Relationships.Type type, EvalStack evaluation)
      {
        this.masterStorage = masterStorage;
        this.slaveStorage = slaveStorage;
        this.type = type;
        this.evaluation = evaluation;
        this.active = false;
      }

      internal DataStorage MasterStorage
      {
        get
        {
          return this.masterStorage;
        }
      }

      internal DataStorage SlaveStorage
      {
        get
        {
          return this.slaveStorage;
        }
      }

      internal EvalStack Evaluation
      {
        get
        {
          return this.evaluation;
        }
      }

      internal Relationships.Type Type
      {
        get
        {
          return this.type;
        }
      }

      internal bool Active
      {
        get
        {
          return this.active;
        }
        set
        {
          this.active = value;
        }
      }

      internal bool Activate(DataStorage pivotStorage)
      {
        if (this.active)
          return pivotStorage.ActivateLink(this);
        return true;
      }

      internal bool Create(DataStorage pivotStorage)
      {
        if (this.active)
          return pivotStorage.CreateLink(this);
        return true;
      }

      internal bool Update(DataStorage pivotStorage)
      {
        if (this.active)
          return pivotStorage.UpdateLink(this);
        return true;
      }

      internal bool Delete(DataStorage pivotStorage)
      {
        if (this.active)
          return pivotStorage.DeleteLink(this);
        return true;
      }

      internal bool Assume(bool toModify)
      {
        if (this.active)
          return this.SlaveStorage.AssumeLink(this, toModify);
        return true;
      }

      internal bool Resume(bool toModify)
      {
        if (this.active)
          return this.SlaveStorage.ResumeLink(this, toModify);
        return true;
      }
    }
  }
}
