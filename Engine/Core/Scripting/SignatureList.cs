using System.Collections.Generic;

namespace VistaDB.Engine.Core.Scripting
{
  internal class SignatureList : List<Signature>
  {
    internal int BREAK = -1;
    internal int PARENTHESIS = -1;
    internal int COLUMN = -1;
    internal int COMMENTS = -1;
    internal int COMMENTS_INLINE = -1;
    internal int IF = -1;
    internal int THEN = -1;
    internal int ELSE = -1;
    internal int CONSTANT = -1;
    internal int LOGICAL_CONST = -1;
    internal int LOGICAL_COMPARE = -1;
    internal int LOGICAL_BITWISE = -1;
    internal int exactEqualDifference;
    private int patternsEntry;
    private int operatorsEntry;
    private int udfEntry;

    internal SignatureList()
      : base(64)
    {
      this.InitList();
      this.patternsEntry = this.Count;
      this.DoInitPatterns();
      this.operatorsEntry = this.Count;
      this.DoInitLogicalOperators();
      this.DoInitMathOperators();
      this.DoInitLanguageOperators();
      this.udfEntry = this.Count;
      this.DoInitUDFs();
      this.FinalizeList();
    }

    internal int Add(Signature item)
    {
      base.Add(item);
      return this.Count - 1;
    }

    internal int PatternsEntry
    {
      get
      {
        return this.patternsEntry;
      }
    }

    internal int OperatorsEntry
    {
      get
      {
        return this.operatorsEntry;
      }
    }

    internal int ExactEqualDifference
    {
      get
      {
        return this.exactEqualDifference;
      }
    }

    private void InitList()
    {
      this.BREAK = 0;
      Signature signature1 = (Signature) new Break(this.BREAK);
      signature1.Entry = this.Add(signature1);
      this.COLUMN = this.Count;
      Signature signature2 = (Signature) new ColumnSignature(this.COLUMN, VistaDBType.NChar);
      signature2.Entry = this.Add(signature2);
      Signature signature3 = (Signature) new ColumnSignature(this.COLUMN, VistaDBType.Bit);
      signature3.Entry = this.Add(signature3);
      Signature signature4 = (Signature) new ColumnSignature(this.COLUMN, VistaDBType.BigInt);
      signature4.Entry = this.Add(signature4);
      Signature signature5 = (Signature) new ColumnSignature(this.COLUMN, VistaDBType.Int);
      signature5.Entry = this.Add(signature5);
      Signature signature6 = (Signature) new ColumnSignature(this.COLUMN, VistaDBType.SmallInt);
      signature6.Entry = this.Add(signature6);
      Signature signature7 = (Signature) new ColumnSignature(this.COLUMN, VistaDBType.TinyInt);
      signature7.Entry = this.Add(signature7);
      Signature signature8 = (Signature) new ColumnSignature(this.COLUMN, VistaDBType.Decimal);
      signature8.Entry = this.Add(signature8);
      Signature signature9 = (Signature) new ColumnSignature(this.COLUMN, VistaDBType.DateTime);
      signature9.Entry = this.Add(signature9);
      Signature signature10 = (Signature) new ColumnSignature(this.COLUMN, VistaDBType.Float);
      signature10.Entry = this.Add(signature10);
      Signature signature11 = (Signature) new ColumnSignature(this.COLUMN, VistaDBType.Real);
      signature11.Entry = this.Add(signature11);
      Signature signature12 = (Signature) new ColumnSignature(this.COLUMN, VistaDBType.VarBinary);
      signature12.Entry = this.Add(signature12);
      Signature signature13 = (Signature) new ColumnSignature(this.COLUMN, VistaDBType.UniqueIdentifier);
      signature13.Entry = this.Add(signature13);
      this.CONSTANT = this.Count;
      Signature signature14 = (Signature) new Constant(this.CONSTANT);
      signature14.Entry = this.Add(signature14);
    }

    private void FinalizeList()
    {
      this.COMMENTS_INLINE = this.Count;
      Signature signature1 = (Signature) new InlineComments("//", this.COMMENTS_INLINE);
      signature1.Entry = this.Add(signature1);
      this.COMMENTS = this.Count;
      Signature signature2 = (Signature) new BeginComments("/*", "*/", this.COMMENTS);
      signature2.Entry = this.Add(signature2);
      Signature signature3 = (Signature) new EndComments("*/", "*/", this.COMMENTS);
      signature3.Entry = this.Add(signature3);
    }

    protected virtual void DoInitPatterns()
    {
      this.PARENTHESIS = this.Count;
      Signature signature1 = (Signature) new ParenthesesOpen("(", this.PARENTHESIS, this.PARENTHESIS + 1);
      signature1.Entry = this.Add(signature1);
      Signature signature2 = (Signature) new ParenthesesClose(")", this.PARENTHESIS, this.PARENTHESIS + 1);
      signature2.Entry = this.Add(signature2);
      Signature signature3 = (Signature) new ParenthesesDelimiter(",", this.PARENTHESIS, this.PARENTHESIS + 1);
      signature3.Entry = this.Add(signature3);
    }

    protected virtual void DoInitLogicalOperators()
    {
      int count = this.Count;
      Signature signature1 = (Signature) new Assign(":", count, VistaDBType.Unknown);
      signature1.Entry = this.Add(signature1);
      Signature signature2 = (Signature) new Assign(":=", count, VistaDBType.Unknown);
      signature2.Entry = this.Add(signature2);
      this.LOGICAL_CONST = this.Count;
      Signature signature3 = (Signature) new TrueSignature("TRUE", this.LOGICAL_CONST);
      signature3.Entry = this.Add(signature3);
      Signature signature4 = (Signature) new TrueSignature("YES", this.LOGICAL_CONST);
      signature4.Entry = this.Add(signature4);
      Signature signature5 = (Signature) new FalseSignature("FALSE", this.LOGICAL_CONST);
      signature5.Entry = this.Add(signature5);
      Signature signature6 = (Signature) new FalseSignature("NO", this.LOGICAL_CONST);
      signature6.Entry = this.Add(signature6);
      Signature signature7 = (Signature) new NullSignature("NULL", this.LOGICAL_CONST);
      signature7.Entry = this.Add(signature7);
      this.LOGICAL_BITWISE = this.Count;
      Signature signature8 = (Signature) new AndBitwise("AND", this.LOGICAL_BITWISE);
      signature8.Entry = this.Add(signature8);
      Signature signature9 = (Signature) new AndBitwise("&&", this.LOGICAL_BITWISE);
      signature9.Entry = this.Add(signature9);
      Signature signature10 = (Signature) new OrBitwise("OR", this.LOGICAL_BITWISE);
      signature10.Entry = this.Add(signature10);
      Signature signature11 = (Signature) new OrBitwise("||", this.LOGICAL_BITWISE);
      signature11.Entry = this.Add(signature11);
      Signature signature12 = (Signature) new XorBitwise("XOR", this.LOGICAL_BITWISE);
      signature12.Entry = this.Add(signature12);
      Signature signature13 = (Signature) new NotBitwise("NOT", this.LOGICAL_BITWISE);
      signature13.Entry = this.Add(signature13);
      Signature signature14 = (Signature) new NotBitwise("!", this.LOGICAL_BITWISE);
      signature14.Entry = this.Add(signature14);
      this.LOGICAL_COMPARE = this.Count;
      Signature signature15 = (Signature) new Equal("=", this.LOGICAL_COMPARE);
      signature15.Entry = this.Add(signature15);
      Signature signature16 = (Signature) new Less("<", this.LOGICAL_COMPARE);
      signature16.Entry = this.Add(signature16);
      Signature signature17 = (Signature) new Great(">", this.LOGICAL_COMPARE);
      signature17.Entry = this.Add(signature17);
      Signature signature18 = (Signature) new LessEqual("<=", this.LOGICAL_COMPARE);
      signature18.Entry = this.Add(signature18);
      Signature signature19 = (Signature) new GreatEqual(">=", this.LOGICAL_COMPARE);
      signature19.Entry = this.Add(signature19);
      Signature signature20 = (Signature) new IsNull("IS", this.LOGICAL_COMPARE);
      signature20.Entry = this.Add(signature20);
      Signature signature21 = (Signature) new ReadOnlySignature(this.Count, this.PARENTHESIS + 1);
      signature21.Entry = this.Add(signature21);
      Signature signature22 = (Signature) new ForeignKeyConstraint(this.Count, this.PARENTHESIS + 1);
      signature22.Entry = this.Add(signature22);
      Signature signature23 = (Signature) new NonreferencedPrimaryKey(this.Count, this.PARENTHESIS + 1);
      signature23.Entry = this.Add(signature23);
    }

    protected virtual void DoInitMathOperators()
    {
      Signature signature1 = (Signature) new Minus("-", this.Count, 1);
      signature1.Entry = this.Add(signature1);
      Signature signature2 = (Signature) new UnaryMinus("-", this.Count);
      signature2.Entry = this.Add(signature2);
      Signature signature3 = (Signature) new Plus("+", this.Count, 1);
      signature3.Entry = this.Add(signature3);
      Signature signature4 = (Signature) new UnaryPlus("+", this.Count);
      signature4.Entry = this.Add(signature4);
      Signature signature5 = (Signature) new Multiplication("*", this.Count);
      signature5.Entry = this.Add(signature5);
      Signature signature6 = (Signature) new Dividing("/", this.Count);
      signature6.Entry = this.Add(signature6);
    }

    protected virtual void DoInitLanguageOperators()
    {
      Signature signature1 = (Signature) new Append(";", this.Count);
      signature1.Entry = this.Add(signature1);
      int count1 = this.Count;
      Signature signature2 = (Signature) new GetDate("NOW", count1, this.PARENTHESIS + 1);
      signature2.Entry = this.Add(signature2);
      Signature signature3 = (Signature) new GetDate("GETDATE", count1, this.PARENTHESIS + 1);
      signature3.Entry = this.Add(signature3);
      Signature signature4 = (Signature) new GetUtcDate("GETUTCDATE", this.Count, this.PARENTHESIS + 1);
      signature4.Entry = this.Add(signature4);
      Signature signature5 = (Signature) new Date("DATE", this.Count, this.PARENTHESIS + 1);
      signature5.Entry = this.Add(signature5);
      Signature signature6 = (Signature) new StringConversion("STRING", this.Count, this.PARENTHESIS + 1);
      signature6.Entry = this.Add(signature6);
      Signature signature7 = (Signature) new FloatConversion("FLOAT", this.Count, this.PARENTHESIS + 1);
      signature7.Entry = this.Add(signature7);
      Signature signature8 = (Signature) new DoubleConversion("DOUBLE", this.Count, this.PARENTHESIS + 1);
      signature8.Entry = this.Add(signature8);
      Signature signature9 = (Signature) new DecimalConversion("DECIMAL", this.Count, this.PARENTHESIS + 1);
      signature9.Entry = this.Add(signature9);
      Signature signature10 = (Signature) new MoneyConversion("MONEY", this.Count, this.PARENTHESIS + 1);
      signature10.Entry = this.Add(signature10);
      Signature signature11 = (Signature) new SmallMoneyConversion("SMALLMONEY", this.Count, this.PARENTHESIS + 1);
      signature11.Entry = this.Add(signature11);
      Signature signature12 = (Signature) new Int64Conversion("LONG", this.Count, this.PARENTHESIS + 1);
      signature12.Entry = this.Add(signature12);
      Signature signature13 = (Signature) new IdentitySignature(this.Count, this.PARENTHESIS + 1);
      signature13.Entry = this.Add(signature13);
      int count2 = this.Count;
      Signature signature14 = (Signature) new GuidSignature("GUID", count2, this.PARENTHESIS + 1);
      signature14.Entry = this.Add(signature14);
      Signature signature15 = (Signature) new GuidSignature("NEWID", count2, this.PARENTHESIS + 1);
      signature15.Entry = this.Add(signature15);
    }

    protected virtual void DoInitUDFs()
    {
    }
  }
}
