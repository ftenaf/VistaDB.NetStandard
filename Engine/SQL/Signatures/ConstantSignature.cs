﻿using System;
using System.Collections.Generic;
using System.Globalization;
using VistaDB.Diagnostic;
using VistaDB.Engine.Core;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ConstantSignature : Signature
  {
    private bool isChanged;
    private object val;

        private ConstantSignature(object val, VistaDBType dataType, SQLParser parser)
      : base(parser)
    {
      this.val = val;
      isAllowNull = this.val == null;
      signatureType = SignatureType.Constant;
      this.dataType = dataType;
      isChanged = true;
      optimizable = true;
    }

    private ConstantSignature(IColumn result, VistaDBType dataType, Statement parent)
      : base(parent)
    {
      this.result = CreateColumn(dataType);
      Convert(result, this.result);
      val = this.result.Value;
      isAllowNull = val == null;
      signatureType = SignatureType.Constant;
      this.dataType = dataType;
      isChanged = true;
      optimizable = true;
    }

    private ConstantSignature(IColumn result, Statement parent)
      : base(parent)
    {
      this.result = result;
      val = this.result.Value;
      isAllowNull = val == null;
      signatureType = SignatureType.Constant;
      dataType = result.Type;
      isChanged = true;
      optimizable = true;
    }

    internal static ConstantSignature CreateSignature(SQLParser parser)
    {
      string token = parser.TokenValue.Token;
      switch (parser.TokenValue.TokenType)
      {
        case TokenType.Unknown:
          if (string.Compare(token, "NULL", StringComparison.OrdinalIgnoreCase) == 0)
            return new ConstantSignature(null, VistaDBType.NChar, parser);
          if (string.Compare(token, "TRUE", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(token, "YES", StringComparison.OrdinalIgnoreCase) == 0)
            return new ConstantSignature(true, VistaDBType.Bit, parser);
          if (string.Compare(token, "FALSE", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(token, "NO", StringComparison.OrdinalIgnoreCase) == 0)
            return new ConstantSignature(false, VistaDBType.Bit, parser);
          break;
        case TokenType.String:
          return new ConstantSignature(token, VistaDBType.NChar, parser);
        case TokenType.Integer:
          return new ConstantSignature(long.Parse(token, NumberStyles.Integer, CrossConversion.NumberFormat), VistaDBType.BigInt, parser);
        case TokenType.Float:
          return new ConstantSignature(double.Parse(token, NumberStyles.Float, CrossConversion.NumberFormat), VistaDBType.Float, parser);
        case TokenType.Binary:
          return new ConstantSignature(Utils.StringToBinary(token), VistaDBType.VarBinary, parser);
      }
      return null;
    }

    internal static ConstantSignature CreateSignature(IColumn column, Statement parent)
    {
      return new ConstantSignature(column, parent);
    }

    internal static ConstantSignature CreateSignature(IColumn column, VistaDBType vistaDBType, Statement parent)
    {
      return new ConstantSignature(column, vistaDBType, parent);
    }

    internal static ConstantSignature CreateSignature(string val, VistaDBType vistaDBType, SQLParser parser)
    {
      return new ConstantSignature(val, vistaDBType, parser);
    }

    protected override IColumn InternalExecute()
    {
      if (isChanged)
      {
                result.Value = val;
        isChanged = false;
      }
      return result;
    }

    protected override void OnSimpleExecute()
    {
      if (!isChanged)
        return;
            result.Value = val;
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      if (dataType == VistaDBType.Bit)
        return constrainOperations.AddLogicalExpression(this);
      return false;
    }

    public override SignatureType OnPrepare()
    {
      return signatureType;
    }

    public override bool HasAggregateFunction(out bool distinct)
    {
      distinct = false;
      return false;
    }

    public override int GetWidth()
    {
      if (!Utils.IsCharacterDataType(dataType))
        return base.GetWidth();
      if (val != null)
        return ((string) val).Length;
      return 0;
    }

    protected override bool IsEquals(Signature signature)
    {
      if (!(signature is ConstantSignature) || signature.DataType != dataType)
        return false;
      ConstantSignature constantSignature = (ConstantSignature) signature;
      if (val == null && constantSignature.val == null)
        return true;
      if (val != null && constantSignature.val != null)
        return ((IComparable) val).CompareTo(constantSignature.val) == 0;
      return false;
    }

    protected override void RelinkParameters(Signature signature, ref int columnCount)
    {
    }

    public override void SetChanged()
    {
      isChanged = true;
    }

    public override void ClearChanged()
    {
      if (result == null)
        return;
      isChanged = false;
    }

    public static Signature PrepareAndCheckConstant(Signature signature, VistaDBType preferenceType)
    {
      if (signature.OnPrepare() != SignatureType.Constant || signature.SignatureType == SignatureType.Constant && (preferenceType == VistaDBType.Unknown || signature.DataType == preferenceType))
        return signature;
      if (preferenceType == VistaDBType.Unknown)
        return new ConstantSignature(signature.Execute(), signature.Parent);
      return new ConstantSignature(signature.Execute(), preferenceType, signature.Parent);
    }

    public static SignatureType PrepareBinaryOperator(ref Signature leftOperand, ref Signature rightOperand, out VistaDBType dataType, bool mustBeNumeric, bool maxNumeric, string text, int lineNo, int symbolNo)
    {
      SignatureType signatureType1 = leftOperand.OnPrepare();
      SignatureType signatureType2 = rightOperand.OnPrepare();
      if (leftOperand.AlwaysNull || rightOperand.AlwaysNull)
      {
        dataType = leftOperand.DataType;
        return SignatureType.Constant;
      }
      dataType = !mustBeNumeric ? Utils.GetMaxDataType(leftOperand.DataType, rightOperand.DataType) : Utils.GetMaxNumericDataType(leftOperand.DataType, rightOperand.DataType);
      if (leftOperand.DataType != VistaDBType.Unknown && !Utils.CompatibleTypes(leftOperand.DataType, dataType))
        throw new VistaDBSQLException(558, text, lineNo, symbolNo);
      if (rightOperand.DataType != VistaDBType.Unknown && !Utils.CompatibleTypes(rightOperand.DataType, dataType))
        throw new VistaDBSQLException(558, text, lineNo, symbolNo);
      if (signatureType1 == SignatureType.Constant && (leftOperand.SignatureType != SignatureType.Constant || leftOperand.DataType != dataType))
        leftOperand = new ConstantSignature(leftOperand.Execute(), dataType, leftOperand.Parent);
      if (signatureType2 == SignatureType.Constant && (rightOperand.SignatureType != SignatureType.Constant || rightOperand.DataType != dataType))
        rightOperand = new ConstantSignature(rightOperand.Execute(), dataType, rightOperand.Parent);
      return signatureType1 == SignatureType.Constant && signatureType2 == SignatureType.Constant ? SignatureType.Constant : SignatureType.Expression;
    }

    public static SignatureType PreparePlusMinusOperator(ref Signature leftOperand, ref Signature rightOperand, out VistaDBType dataType, out IColumn leftValue, out IColumn rightValue, out bool dateOperands, bool isPlusOperator, string text, int lineNo, int symbolNo)
    {
      SignatureType signatureType1 = leftOperand.OnPrepare();
      SignatureType signatureType2 = rightOperand.OnPrepare();
      if (leftOperand.AlwaysNull || rightOperand.AlwaysNull)
      {
        dateOperands = false;
        dataType = leftOperand.DataType;
        leftValue = null;
        rightValue = null;
        return SignatureType.Constant;
      }
      int num = Utils.CompareRank(leftOperand.DataType, rightOperand.DataType);
      dataType = num < 0 ? rightOperand.DataType : leftOperand.DataType;
      dateOperands = Utils.IsDateDataType(dataType);
      VistaDBType dataType1;
      VistaDBType dataType2;
      if (dateOperands)
      {
        VistaDBType dataType3;
        if (num < 0)
        {
          dataType1 = VistaDBType.Float;
          dataType2 = dataType;
          dataType3 = leftOperand.DataType;
        }
        else
        {
          dataType1 = dataType;
          dataType2 = VistaDBType.Float;
          dataType3 = rightOperand.DataType;
        }
        if (num < 0 && !isPlusOperator || !Utils.CompatibleTypes(dataType3, VistaDBType.Float))
          throw new VistaDBSQLException(558, text, lineNo, symbolNo);
      }
      else
      {
        if (!Utils.CompatibleTypes(leftOperand.DataType, dataType) || !Utils.CompatibleTypes(rightOperand.DataType, dataType))
          throw new VistaDBSQLException(558, text, lineNo, symbolNo);
        dataType1 = dataType;
        dataType2 = dataType;
      }
      if (signatureType1 == SignatureType.Constant && (leftOperand.SignatureType != SignatureType.Constant || leftOperand.DataType != dataType1))
        leftOperand = new ConstantSignature(leftOperand.Execute(), dataType1, leftOperand.Parent);
      if (signatureType2 == SignatureType.Constant && (rightOperand.SignatureType != SignatureType.Constant || rightOperand.DataType != dataType2))
        rightOperand = new ConstantSignature(rightOperand.Execute(), dataType2, rightOperand.Parent);
      leftValue = leftOperand.CreateColumn(dataType1);
      rightValue = leftOperand.CreateColumn(dataType2);
      return leftOperand.SignatureType == SignatureType.Constant && rightOperand.SignatureType == SignatureType.Constant ? SignatureType.Constant : SignatureType.Expression;
    }

    public override void GetAggregateFunctions(List<AggregateFunction> list)
    {
    }

    public override bool AlwaysNull
    {
      get
      {
        return val == null;
      }
    }

    protected override bool InternalGetIsChanged()
    {
      return isChanged;
    }

    public override int ColumnCount
    {
      get
      {
        return 0;
      }
    }
  }
}
