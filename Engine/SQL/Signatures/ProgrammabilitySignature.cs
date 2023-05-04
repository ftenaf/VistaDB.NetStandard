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
      parameters = new List<Signature>();
      paramValues = (IColumn[]) null;
      parameterTypes = (VistaDBType[]) null;
      signatureType = SignatureType.Expression;
      skipNull = true;
    }

    protected ProgrammabilitySignature(SQLParser parser, int paramCount, bool needSkip)
      : base(parser)
    {
      if (needSkip)
        parser.SkipToken(true);
      ParseParameters(parser);
      if (paramCount >= 0 && paramCount != parameters.Count)
        throw new VistaDBSQLException(501, text, lineNo, symbolNo);
      paramValues = new IColumn[parameters.Count];
      parameterTypes = new VistaDBType[parameters.Count];
      signatureType = SignatureType.Expression;
      skipNull = true;
    }

    protected abstract void ParseParameters(SQLParser parser);

    protected abstract object ExecuteSubProgram();

    protected Signature this[int i]
    {
      get
      {
        return parameters[i];
      }
      set
      {
        parameters[i] = value;
      }
    }

    protected int ParamCount
    {
      get
      {
        return parameters.Count;
      }
    }

    public override bool AlwaysNull
    {
      get
      {
        if (!skipNull)
          return false;
        for (int index = 0; index < parameters.Count; ++index)
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
        for (int index = 0; index < parameters.Count; ++index)
          num += this[index].ColumnCount;
        return num;
      }
    }

    protected override IColumn InternalExecute()
    {
      object resValue;
      if (PrepareExecute(out resValue) && dataType != VistaDBType.Unknown)
        ((IValue) result).Value = resValue;
      return result;
    }

    public override void SetChanged()
    {
      for (int index = 0; index < parameters.Count; ++index)
        this[index].SetChanged();
    }

    public override void SwitchToTempTable(SourceRow sourceRow, int columnIndex, SelectStatement.ResultColumn resultColumn)
    {
      int index = parameters.IndexOf(resultColumn.Signature);
      if (index < 0)
        return;
      parameters[index].SwitchToTempTable(sourceRow, columnIndex);
    }

    public override void ClearChanged()
    {
      for (int index = 0; index < parameters.Count; ++index)
        this[index].ClearChanged();
    }

    protected override bool InternalGetIsChanged()
    {
      int count = parameters.Count;
      bool flag = count == 0;
      for (int index = 0; !flag && index < count; ++index)
        flag = this[index].GetIsChanged();
      return flag;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (GetType() != signature.GetType())
        return false;
      ProgrammabilitySignature programmabilitySignature = (ProgrammabilitySignature) signature;
      if (parameters.Count != programmabilitySignature.parameters.Count)
        return false;
      for (int index = 0; index < parameters.Count; ++index)
      {
        if (this[index] != programmabilitySignature[index] || parameterTypes[index] != programmabilitySignature.parameterTypes[index])
          return false;
      }
      return true;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
      for (int index = 0; index < parameters.Count; ++index)
        this[index].GetAggregateFunctions(list);
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      bool flag = false;
      distinct = false;
      for (int index = 0; index < parameters.Count; ++index)
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
      for (int index = 0; index < parameters.Count; ++index)
        this[index] = this[index].Relink(signature, ref columnCount);
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = SignatureType.Constant;
      for (int index = 0; index < parameters.Count; ++index)
      {
        if (!(this[index] == (Signature) null))
        {
          this[index] = ConstantSignature.PrepareAndCheckConstant(this[index], parameterTypes[index]);
          if (parameterTypes[index] != VistaDBType.Unknown && !Utils.CompatibleTypes(this[index].DataType, parameterTypes[index]))
            throw new VistaDBSQLException(550, text, lineNo, symbolNo);
          if (signatureType == SignatureType.Constant && this[index].SignatureType != SignatureType.Constant)
            signatureType = SignatureType.Expression;
          paramValues[index] = parameterTypes[index] == VistaDBType.Unknown ? CreateColumn(VistaDBType.NChar) : CreateColumn(parameterTypes[index]);
        }
      }
      if (signatureType != SignatureType.Constant && !AlwaysNull)
        return this.signatureType;
      return SignatureType.Constant;
    }

    internal void SetReturnParameter(IParameter param)
    {
      returnParameter = param;
    }

    protected bool PrepareExecute(out object resValue)
    {
      if (GetIsChanged())
      {
        if (parameters.Count > 0)
        {
          bool flag = false;
          for (int index = 0; index < parameters.Count; ++index)
          {
            Signature signature = this[index];
            if (!(signature == (Signature) null))
            {
              if (signature.SignatureType == SignatureType.MultiplyColumn)
              {
                StringBuilder stringBuilder = new StringBuilder();
                SourceTable sourceTable = parent.GetSourceTable(0);
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
                ((IValue) paramValues[index]).Value = (object) stringBuilder.ToString();
              }
              else
              {
                signature.Execute();
                if (signature.Result.IsNull)
                {
                  if (skipNull)
                  {
                    flag = true;
                    break;
                  }
                }
                try
                {
                  Convert((IValue) signature.Result, (IValue) paramValues[index]);
                }
                catch (Exception ex)
                {
                  throw new VistaDBSQLException(ex, 663, signature.Parent.Name, signature.LineNo, signature.SymbolNo);
                }
              }
            }
          }
          resValue = !flag || !skipNull ? ExecuteSubProgram() : (object) null;
        }
        else
          resValue = ExecuteSubProgram();
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
