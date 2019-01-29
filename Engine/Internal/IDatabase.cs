using System;
using System.Reflection;
using VistaDB.DDA;
using VistaDB.Engine.Core;

namespace VistaDB.Engine.Internal
{
  internal interface IDatabase : IVistaDBDatabase, IVistaDBTable, IDisposable
  {
    IConversion Conversion { get; }

    IRow GetRowStructure(string tableName);

    IColumn CreateEmptyColumn(VistaDBType type);

    IColumn CreateEmtpyUnicodeColumn();

    IView CreateViewInstance(string name);

    void CreateViewObject(IView view);

    void DeleteViewObject(IView view);

    IViewList EnumViews();

    bool TryGetProcedure(string procedureName, out ClrHosting.ClrProcedure procedure);

    IStoredProcedureCollection GetStoredProcedures();

    IStoredProcedureInformation CreateStoredProcedureInstance(string name, string script, string descrition);

    void CreateStoredProcedureObject(IStoredProcedureInformation sp);

    void DeleteStoredProcedureObject(string name);

    IUserDefinedFunctionCollection GetUserDefinedFunctions();

    IUserDefinedFunctionInformation CreateUserDefinedFunctionInstance(string name, string script, bool scalarValued, string description);

    void CreateUserDefinedFunctionObject(IUserDefinedFunctionInformation udf);

    void DeleteUserDefinedFunctionObject(string name);

    char MaximumChar { get; }

    MethodInfo PrepareInvoke(string clrProcedure, out MethodInfo fillRow);

    InsensitiveHashtable GetRelationships(string tableName, bool insert, bool delete);

    IVistaDBTable CreateTemporaryTable(IVistaDBTableSchema schema);

    IColumn GetTableAnchor(string tableName);
  }
}
