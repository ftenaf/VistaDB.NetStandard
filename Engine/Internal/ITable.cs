using System;
using VistaDB.DDA;

namespace VistaDB.Engine.Internal
{
    internal interface ITable : IVistaDBTable, IDisposable
    {
        string Alias { get; }
        new IRow CurrentRow { get; set; }

        new IRow CurrentKey { get; set; }

        bool SuppressErrors { get; set; }

        IRow KeyStructure(string indexName);

        string PKIndex { get; }

        void CreateSparseIndex(string name, string keyExpression);

        bool IsReadOnly { get; }

        bool IsExclusive { get; }

        void FreezeSelfRelationships();

        void DefreezeSelfRelationships();

        void PrepareTriggers(TriggerAction eventType);

        void ExecuteTriggers(TriggerAction eventType, bool justReset);

        bool AllowPooling { get; }

        IOptimizedFilter BuildFilterMap(string indexName, IRow lowScopeValue, IRow highScopeValue, bool excludeNulls);

        void BeginOptimizedFiltering(IOptimizedFilter filter, string pivotIndex);

        void ResetOptimizedFiltering();

        void PrepareFtsOptimization();

        void ClearCachedBitmaps();

        new void Post();

        new void Delete();
    }
}
