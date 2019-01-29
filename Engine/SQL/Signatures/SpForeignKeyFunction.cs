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
      if (this.ParamCount > 0)
        throw new VistaDBSQLException(501, "SP_INDEXES", this.lineNo, this.symbolNo);
      this.resultColumnTypes[0] = VistaDBType.NVarChar;
      this.resultColumnTypes[1] = VistaDBType.NVarChar;
      this.resultColumnTypes[2] = VistaDBType.NVarChar;
      this.resultColumnTypes[3] = VistaDBType.NVarChar;
      this.resultColumnTypes[4] = VistaDBType.SmallInt;
      this.resultColumnTypes[5] = VistaDBType.SmallInt;
      this.resultColumnTypes[6] = VistaDBType.SmallInt;
      this.resultColumnTypes[7] = VistaDBType.NVarChar;
      this.resultColumnTypes[8] = VistaDBType.NVarChar;
      this.resultColumnTypes[9] = VistaDBType.NVarChar;
      this.resultColumnNames[0] = "PKTABLE_NAME";
      this.resultColumnNames[1] = "PKCOLUMN_NAME";
      this.resultColumnNames[2] = "FKTABLE_NAME";
      this.resultColumnNames[3] = "FKCOLUMN_NAME";
      this.resultColumnNames[4] = "KEY_SEQ";
      this.resultColumnNames[5] = "UPDATE_RULE";
      this.resultColumnNames[6] = "DELETE_RULE";
      this.resultColumnNames[7] = "FK_NAME";
      this.resultColumnNames[8] = "PK_NAME";
      this.resultColumnNames[9] = "DEFERRABILITY";
    }

    public override bool First(IRow row)
    {
      this.enumerator.Reset();
      if (!this.enumerator.MoveNext())
        return false;
      this.FillRow(row, this.enumerator.Current as IVistaDBRelationshipInformation, 0);
      return true;
    }

    private void FillRow(IRow row, IVistaDBRelationshipInformation relationShipInfo, int keyColumnIndex)
    {
      ((IValue) row[0]).Value = (object) relationShipInfo.PrimaryTable;
      ((IValue) row[2]).Value = (object) relationShipInfo.ForeignTable;
      ((IValue) row[3]).Value = (object) relationShipInfo.ForeignKey;
      ((IValue) row[5]).Value = (object) (short) relationShipInfo.UpdateIntegrity;
      ((IValue) row[6]).Value = (object) (short) relationShipInfo.DeleteIntegrity;
      ((IValue) row[7]).Value = (object) relationShipInfo.Name;
    }

    public override bool GetNextResult(IRow row)
    {
      if (!this.enumerator.MoveNext())
        return false;
      this.FillRow(row, this.enumerator.Current as IVistaDBRelationshipInformation, 0);
      return true;
    }

    public override void Close()
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    protected override object ExecuteSubProgram()
    {
      this.relationships = this.parent.Database.Relationships;
      this.enumerator = (IEnumerator) this.relationships.GetEnumerator();
      return (object) null;
    }
  }
}
