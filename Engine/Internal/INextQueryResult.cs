using System;

namespace VistaDB.Engine.Internal
{
  internal interface INextQueryResult : IDisposable
  {
    IQueryResult ResultSet { get; }

    IQuerySchemaInfo Schema { get; }

    long AffectedRows { get; }
  }
}
