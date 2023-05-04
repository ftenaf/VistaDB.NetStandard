using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class BinaryCompareOperator : BinaryOperator
  {
    protected bool checkAll = true;
    protected VistaDBType operandType;
    protected bool rightOperandIsSubQuery;

    public BinaryCompareOperator(Signature leftOperand, SQLParser parser)
      : base(leftOperand, parser, 2)
    {
      dataType = VistaDBType.Bit;
      operandType = VistaDBType.Bit;
    }

    public override void SwitchToTempTable(SourceRow sourceRow, int columnIndex, SelectStatement.ResultColumn resultColumn)
    {
      if (leftOperand.Equals((object) resultColumn.Signature))
        leftOperand.SwitchToTempTable(sourceRow, columnIndex);
      if (!rightOperand.Equals((object) resultColumn.Signature))
        return;
      rightOperand.SwitchToTempTable(sourceRow, columnIndex);
    }

    protected override void DoParseRightOperand(SQLParser parser, int priority)
    {
      parser.SkipToken(true);
      if (parser.IsToken("ALL"))
      {
        parser.SkipToken(true);
        rightOperandIsSubQuery = true;
      }
      else if (parser.IsToken("SOME") || parser.IsToken("ANY"))
      {
        parser.SkipToken(true);
        rightOperandIsSubQuery = true;
        checkAll = false;
      }
      rightOperand = parser.NextSignature(false, true, 2);
      if (rightOperandIsSubQuery)
      {
        if (!(rightOperand is SubQuerySignature))
          throw new VistaDBSQLException(507, "subquery", rightOperand.LineNo, rightOperand.SymbolNo);
      }
      else
      {
        if (!(rightOperand is SubQuerySignature))
          return;
        rightOperandIsSubQuery = true;
      }
    }

    protected override IColumn InternalExecute()
    {
      if (!GetIsChanged())
        return result;
      ((IValue) result).Value = (object) null;
      needsEvaluation = false;
      if (leftOperand.AlwaysNull || rightOperand.AlwaysNull)
        return result;
      IColumn column1 = leftOperand.Execute();
      if (column1.IsNull)
        return result;
      if (rightOperandIsSubQuery)
      {
        leftValue = column1;
      }
      else
      {
        IColumn column2 = rightOperand.Execute();
        if (column2.IsNull)
          return result;
        Convert((IValue) column1, (IValue) leftValue);
        Convert((IValue) column2, (IValue) rightValue);
      }
      if (Utils.IsCharacterDataType(operandType))
      {
        ((IValue) leftValue).Value = (object) ((string) ((IValue) leftValue).Value).TrimEnd();
        if (!rightOperandIsSubQuery)
          ((IValue) rightValue).Value = (object) ((string) ((IValue) rightValue).Value).TrimEnd();
      }
      ((IValue) result).Value = (object) CompareOperands();
      return result;
    }

    public override SignatureType OnPrepare()
    {
      SignatureType signatureType = ConstantSignature.PrepareBinaryOperator(ref leftOperand, ref rightOperand, out operandType, false, true, text, lineNo, symbolNo);
      leftValue = CreateColumn(operandType);
      rightValue = CreateColumn(operandType);
      if (leftOperand.SignatureType == SignatureType.Column && rightOperand.SignatureType == SignatureType.Column)
        ((ColumnSignature) leftOperand).Table.AddJoinedTable(((ColumnSignature) rightOperand).Table);
      return signatureType;
    }

    protected override bool OnOptimize(ConstraintOperations constrainOperations)
    {
      return constrainOperations.AddLogicalCompare(leftOperand, rightOperand, GetCompareOperation(), GetRevCompareOperation(), false);
    }

    public override bool IsNull
    {
      get
      {
        if (!leftOperand.IsNull)
          return rightOperand.IsNull;
        return true;
      }
    }

    protected abstract bool CompareOperands();

    protected abstract CompareOperation GetCompareOperation();

    protected abstract CompareOperation GetRevCompareOperation();

    protected void CalcOptimizeLevel()
    {
      if (!leftOperand.Optimizable || !rightOperand.Optimizable)
      {
        optimizable = false;
      }
      else
      {
        switch (leftOperand.SignatureType)
        {
          case SignatureType.Constant:
          case SignatureType.Column:
          case SignatureType.Parameter:
          case SignatureType.ExternalColumn:
            switch (rightOperand.SignatureType)
            {
              case SignatureType.Constant:
              case SignatureType.Column:
              case SignatureType.Parameter:
              case SignatureType.ExternalColumn:
                optimizable = true;
                return;
              default:
                optimizable = false;
                return;
            }
          default:
            optimizable = false;
            break;
        }
      }
    }
  }
}
