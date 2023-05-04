using System.Collections.Generic;
using VistaDB.Engine.Core.Scripting;

namespace VistaDB.Engine.Core
{
  internal class Relationships : List<Relationships.Relation>
  {
    private int freezeCounter;

    internal void AddRelation(DataStorage masterStorage, DataStorage slaveStorage, Type type, EvalStack linking, bool maxPriority)
    {
      foreach (Relation relation in (List<Relation>) this)
      {
        if (relation.MasterStorage == masterStorage && relation.SlaveStorage == slaveStorage)
          return;
      }
            Relation relation1 = new Relation(masterStorage, slaveStorage, type, linking);
      if (maxPriority)
        Insert(0, relation1);
      else
        Add(relation1);
    }

    internal void RemoveRelation(DataStorage masterStorage, DataStorage slaveStorage)
    {
      for (int index = Count - 1; index >= 0; --index)
      {
                Relation relation = this[index];
        if (relation.MasterStorage == masterStorage && relation.SlaveStorage == slaveStorage)
        {
          RemoveAt(index);
          break;
        }
      }
    }

    internal bool Freeze(bool currentStatus)
    {
      if (++freezeCounter > 1)
        return currentStatus;
      bool flag = false;
      foreach (Relation relation in (List<Relation>) this)
      {
        relation.Active = false;
        flag = flag || relation.Active;
      }
      return flag;
    }

    internal bool Defreeze(bool currentStatus)
    {
      if (--freezeCounter > 0)
        return currentStatus;
      freezeCounter = 0;
      foreach (Relation relation in (List<Relation>) this)
        relation.Active = true;
      return true;
    }

    internal bool MaskRelationship(DataStorage masterStorage, DataStorage slaveStorage, bool activate)
    {
      foreach (Relation relation in (List<Relation>) this)
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
      private Type type;
      private bool active;

      internal Relation(DataStorage masterStorage, DataStorage slaveStorage, Type type, EvalStack evaluation)
      {
        this.masterStorage = masterStorage;
        this.slaveStorage = slaveStorage;
        this.type = type;
        this.evaluation = evaluation;
        active = false;
      }

      internal DataStorage MasterStorage
      {
        get
        {
          return masterStorage;
        }
      }

      internal DataStorage SlaveStorage
      {
        get
        {
          return slaveStorage;
        }
      }

      internal EvalStack Evaluation
      {
        get
        {
          return evaluation;
        }
      }

      internal Type Type
      {
        get
        {
          return type;
        }
      }

      internal bool Active
      {
        get
        {
          return active;
        }
        set
        {
          active = value;
        }
      }

      internal bool Activate(DataStorage pivotStorage)
      {
        if (active)
          return pivotStorage.ActivateLink(this);
        return true;
      }

      internal bool Create(DataStorage pivotStorage)
      {
        if (active)
          return pivotStorage.CreateLink(this);
        return true;
      }

      internal bool Update(DataStorage pivotStorage)
      {
        if (active)
          return pivotStorage.UpdateLink(this);
        return true;
      }

      internal bool Delete(DataStorage pivotStorage)
      {
        if (active)
          return pivotStorage.DeleteLink(this);
        return true;
      }

      internal bool Assume(bool toModify)
      {
        if (active)
          return SlaveStorage.AssumeLink(this, toModify);
        return true;
      }

      internal bool Resume(bool toModify)
      {
        if (active)
          return SlaveStorage.ResumeLink(this, toModify);
        return true;
      }
    }
  }
}
