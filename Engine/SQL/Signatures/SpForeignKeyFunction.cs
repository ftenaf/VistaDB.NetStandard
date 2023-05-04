using System;
using System.Collections;
using VistaDB.DDA;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpForeignKeyFunction : SpecialFunction
  {
    private IVistaDBRelationshipCollection relationships;

    internal SpForeignKeyFunction(SQLParser parser)
      : base(parser, 0, 10)
    {
      if (ParamCount > 0)
        throw new VistaDBSQLException(501, "SP_INDEXES", lineNo, symbolNo);
      resultColumnTypes[0] = VistaDBType.NVarChar;
      resultColumnTypes[1] = VistaDBType.NVarChar;
      resultColumnTypes[2] = VistaDBType.NVarChar;
      resultColumnTypes[3] = VistaDBType.NVarChar;
      resultColumnTypes[4] = VistaDBType.SmallInt;
      resultColumnTypes[5] = VistaDBType.SmallInt;
      resultColumnTypes[6] = VistaDBType.SmallInt;
      resultColumnTypes[7] = VistaDBType.NVarChar;
      resultColumnTypes[8] = VistaDBType.NVarChar;
      resultColumnTypes[9] = VistaDBType.NVarChar;
      resultColumnNames[0] = "PKTABLE_NAME";
      resultColumnNames[1] = "PKCOLUMN_NAME";
      resultColumnNames[2] = "FKTABLE_NAME";
      resultColumnNames[3] = "FKCOLUMN_NAME";
      resultColumnNames[4] = "KEY_SEQ";
      resultColumnNames[5] = "UPDATE_RULE";
      resultColumnNames[6] = "DELETE_RULE";
      resultColumnNames[7] = "FK_NAME";
      resultColumnNames[8] = "PK_NAME";
      resultColumnNames[9] = "DEFERRABILITY";
    }

    public override bool First(IRow row)
    {
      enumerator.Reset();
      if (!enumerator.MoveNext())
        return false;
      FillRow(row, enumerator.Current as IVistaDBRelationshipInformation, 0);
      return true;
    }

    private void FillRow(IRow row, IVistaDBRelationshipInformation relationShipInfo, int keyColumnIndex)
    {
            row[0].Value = relationShipInfo.PrimaryTable;
            row[2].Value = relationShipInfo.ForeignTable;
            row[3].Value = relationShipInfo.ForeignKey;
            row[5].Value = (short)relationShipInfo.UpdateIntegrity;
            row[6].Value = (short)relationShipInfo.DeleteIntegrity;
            row[7].Value = relationShipInfo.Name;
    }

    public override bool GetNextResult(IRow row)
    {
      if (!enumerator.MoveNext())
        return false;
      FillRow(row, enumerator.Current as IVistaDBRelationshipInformation, 0);
      return true;
    }

    public override void Close()
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    protected override object ExecuteSubProgram()
    {
      relationships = parent.Database.Relationships;
      enumerator = relationships.GetEnumerator();
      return null;
    }
  }
}
