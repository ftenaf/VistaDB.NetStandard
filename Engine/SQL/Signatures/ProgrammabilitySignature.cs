using System;
using System.Collections.Generic;
using System.Text;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class ProgrammabilitySignature : Signature
  {
    protected List<Signature> parameters;
    protected IColumn[] paramValues;
    protected VistaDBType[] parameterTypes;
    protected bool skipNull;
    protected IParameter returnParameter;

    protected ProgrammabilitySignature(SQLParser parser)
      : base(parser)
    {
      this.parameters = new List<Signature>();
      this.paramValues = (IColumn[]) null;
      this.parameterTypes = (VistaDBType[]) null;
      this.signatureType = SignatureType.Expression;
      this.skipNull = true;
    }

    protected ProgrammabilitySignature(SQLParser parser, int paramCount, bool needSkip)
      : base(parser)
    {
      if (needSkip)
        parser.SkipToken(true);
      this.ParseParameters(parser);
      if (paramCount >= 0 && paramCount != this.parameters.Count)
        throw new VistaDBSQLException(501, this.text, this.lineNo, this.symbolNo);
      this.paramValues = new IColumn[this.parameters.Count];
      this.parameterTypes = new VistaDBType[this.parameters.Count];
      this.signatureType = SignatureType.Expression;
      this.skipNull = true;
    }

    protected abstract void ParseParameters(SQLParser parser);

    protected abstract object ExecuteSubProgram();

    protected Signature this[int i]
    {
      get
      {
        return this.parameters[i];
      }
      set
      {
        this.parameters[i] = value;
      }
    }

    protected int ParamCount
    {
      get
      {
        return this.parameters.Count;
      }
    }

    public override bool AlwaysNull
    {
      get
      {
        if (!this.skipNull)
          return false;
        for (int index = 0; index < this.parameters.Count; ++index)
        {
          if (this[index].AlwaysNull)
            return true;
        }
        return false;
      }
    }

    public override int ColumnCount
    {
      get
      {
        int num = 0;
        for (int index = 0; index < this.parameters.Count; ++index)
          num += this[index].ColumnCount;
        return num;
      }
    }

    protected override IColumn InternalExecute()
    {
      object resValue;
      if (this.PrepareExecute(out resValue) && this.dataType != VistaDBType.Unknown)
        ((IValue) this.result).Value = resValue;
      return this.result;
    }

    public override void SetChanged()
    {
      for (int index = 0; index < this.parameters.Count; ++index)
        this[index].SetChanged();
    }

    public override void SwitchToTempTable(SourceRow sourceRow, int columnIndex, SelectStatement.ResultColumn resultColumn)
    {
      int index = this.parameters.IndexOf(resultColumn.Signature);
      if (index < 0)
        return;
      this.parameters[index].SwitchToTempTable(sourceRow, columnIndex);
    }

    public override void ClearChanged()
    {
      for (int index = 0; index < this.parameters.Count; ++index)
        this[index].ClearChanged();
    }

    protected override bool InternalGetIsChanged()
    {
      int count = this.parameters.Count;
      bool flag = count == 0;
      for (int index = 0; !flag && index < count; ++index)
        flag = this[index].GetIsChanged();
      return flag;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (this.GetType() != signature.GetType())
        return false;
      ProgrammabilitySignature programmabilitySignature = (ProgrammabilitySignature) signature;
      if (this.parameters.Count != programmabilitySignature.parameters.Count)
        return false;
      for (int index = 0; index < this.parameters.Count; ++index)
      {
        if (this[index] != programmabilitySignature[index] || this.parameterTypes[index] != programmabilitySignature.parameterTypes[index])
          return false;
      }
      return true;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      for (int index = 0; index < this.parameters.Count; ++index)
        this[index].GetAggregateFunctions(list);
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      bool flag = false;
      distinct = false;
      for (int index = 0; index < this.parameters.Count; ++index)
      {
        if (!(this[index] == (Signature) null) && this[index].HasAggregateFunction(out distinct))
        {
          if (distinct)
            return true;
          flag = true;
        }
      }
      return flag;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
      for (int index = 0; index < this.parameters.Count; ++index)
        this[index] = this[index].Relink(signature, ref columnCount);
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = SignatureType.Constant;
      for (int index = 0; index < this.parameters.Count; ++index)
      {
        if (!(this[index] == (Signature) null))
        {
          this[index] = ConstantSignature.PrepareAndCheckConstant(this[index], this.parameterTypes[index]);
          if (this.parameterTypes[index] != VistaDBType.Unknown && !Utils.CompatibleTypes(this[index].DataType, this.parameterTypes[index]))
            throw new VistaDBSQLException(550, this.text, this.lineNo, this.symbolNo);
          if (signatureType == SignatureType.Constant && this[index].SignatureType != SignatureType.Constant)
            signatureType = SignatureType.Expression;
          this.paramValues[index] = this.parameterTypes[index] == VistaDBType.Unknown ? this.CreateColumn(VistaDBType.NChar) : this.CreateColumn(this.parameterTypes[index]);
        }
      }
      if (signatureType != SignatureType.Constant && !this.AlwaysNull)
        return this.signatureType;
      return SignatureType.Constant;
    }

    internal void SetReturnParameter(IParameter param)
    {
      this.returnParameter = param;
    }

    protected bool PrepareExecute(out object resValue)
    {
      if (this.GetIsChanged())
      {
        if (this.parameters.Count > 0)
        {
          bool flag = false;
          for (int index = 0; index < this.parameters.Count; ++index)
          {
            Signature signature = this[index];
            if (!(signature == (Signature) null))
            {
              if (signature.SignatureType == SignatureType.MultiplyColumn)
              {
                StringBuilder stringBuilder = new StringBuilder();
                SourceTable sourceTable = this.parent.GetSourceTable(0);
                int colIndex = 0;
                for (int columnCount = sourceTable.GetColumnCount(); colIndex < columnCount; ++colIndex)
                {
                  IColumn column = sourceTable.SimpleGetColumn(colIndex);
                  if (column.SystemType == typeof (string) && !column.IsNull)
                  {
                    stringBuilder.Append(((IValue) column).Value.ToString());
                    stringBuilder.Append(' ');
                  }
                }
                ((IValue) this.paramValues[index]).Value = (object) stringBuilder.ToString();
              }
              else
              {
                signature.Execute();
                if (signature.Result.IsNull)
                {
                  if (this.skipNull)
                  {
                    flag = true;
                    break;
                  }
                }
                try
                {
                  this.Convert((IValue) signature.Result, (IValue) this.paramValues[index]);
                }
                catch (Exception ex)
                {
                  throw new VistaDBSQLException(ex, 663, signature.Parent.Name, signature.LineNo, signature.SymbolNo);
                }
              }
            }
          }
          resValue = !flag || !this.skipNull ? this.ExecuteSubProgram() : (object) null;
        }
        else
          resValue = this.ExecuteSubProgram();
        return true;
      }
      resValue = (object) null;
      return false;
    }

    internal virtual void DisposeSubProgramStatement()
    {
    }
  }
}
