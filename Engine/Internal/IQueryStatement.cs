using System;
using System.Data;
using VistaDB.Provider;

namespace VistaDB.Engine.Internal
{
  internal interface IQueryStatement : IDisposable
  {
    long Id { get; }

    string CommandText { get; }

    string Name { get; }

    IQueryStatement SubQuery(int index);

    int SubQueryCount { get; }

    VistaDBType PrepareQuery();

    IQueryResult ExecuteQuery();

    IQuerySchemaInfo GetSchemaInfo();

    void ResetResult();

    INextQueryResult NextResult(VistaDBPipe pipe);

    void DoSetParam(string paramName, object val, VistaDBType dataType, ParameterDirection directional);

    void DoSetParam(string paramName, IParameter param);

    IParameter DoGetParam(string paramName);

    IParameter DoGetReturnParameter();

    void DoSetReturnParameter(IParameter param);

    void DoClearParams();

    long AffectedRows { get; }

    bool HasDDLCommands { get; }

    bool Disposed { get; }

    bool LockedDisposing { get; set; }
  }
}
