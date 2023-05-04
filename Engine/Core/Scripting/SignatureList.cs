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
        private readonly int patternsEntry;
        private readonly int operatorsEntry;
        private readonly int udfEntry;

        internal SignatureList()
          : base(64)
        {
            InitList();
            patternsEntry = Count;
            DoInitPatterns();
            operatorsEntry = Count;
            DoInitLogicalOperators();
            DoInitMathOperators();
            DoInitLanguageOperators();
            udfEntry = Count;
            DoInitUDFs();
            FinalizeList();
        }

        internal new int Add(Signature item)
        {
            base.Add(item);
            return Count - 1;
        }

        internal int PatternsEntry
        {
            get
            {
                return patternsEntry;
            }
        }

        internal int OperatorsEntry
        {
            get
            {
                return operatorsEntry;
            }
        }

        internal int ExactEqualDifference
        {
            get
            {
                return exactEqualDifference;
            }
        }

        private void InitList()
        {
            BREAK = 0;
            Signature signature1 = new Break(BREAK);
            signature1.Entry = Add(signature1);
            COLUMN = Count;
            Signature signature2 = new ColumnSignature(COLUMN, VistaDBType.NChar);
            signature2.Entry = Add(signature2);
            Signature signature3 = new ColumnSignature(COLUMN, VistaDBType.Bit);
            signature3.Entry = Add(signature3);
            Signature signature4 = new ColumnSignature(COLUMN, VistaDBType.BigInt);
            signature4.Entry = Add(signature4);
            Signature signature5 = new ColumnSignature(COLUMN, VistaDBType.Int);
            signature5.Entry = Add(signature5);
            Signature signature6 = new ColumnSignature(COLUMN, VistaDBType.SmallInt);
            signature6.Entry = Add(signature6);
            Signature signature7 = new ColumnSignature(COLUMN, VistaDBType.TinyInt);
            signature7.Entry = Add(signature7);
            Signature signature8 = new ColumnSignature(COLUMN, VistaDBType.Decimal);
            signature8.Entry = Add(signature8);
            Signature signature9 = new ColumnSignature(COLUMN, VistaDBType.DateTime);
            signature9.Entry = Add(signature9);
            Signature signature10 = new ColumnSignature(COLUMN, VistaDBType.Float);
            signature10.Entry = Add(signature10);
            Signature signature11 = new ColumnSignature(COLUMN, VistaDBType.Real);
            signature11.Entry = Add(signature11);
            Signature signature12 = new ColumnSignature(COLUMN, VistaDBType.VarBinary);
            signature12.Entry = Add(signature12);
            Signature signature13 = new ColumnSignature(COLUMN, VistaDBType.UniqueIdentifier);
            signature13.Entry = Add(signature13);
            CONSTANT = Count;
            Signature signature14 = new Constant(CONSTANT);
            signature14.Entry = Add(signature14);
        }

        private void FinalizeList()
        {
            COMMENTS_INLINE = Count;
            Signature signature1 = new InlineComments("//", COMMENTS_INLINE);
            signature1.Entry = Add(signature1);
            COMMENTS = Count;
            Signature signature2 = new BeginComments("/*", "*/", COMMENTS);
            signature2.Entry = Add(signature2);
            Signature signature3 = new EndComments("*/", "*/", COMMENTS);
            signature3.Entry = Add(signature3);
        }

        protected virtual void DoInitPatterns()
        {
            PARENTHESIS = Count;
            Signature signature1 = new ParenthesesOpen("(", PARENTHESIS, PARENTHESIS + 1);
            signature1.Entry = Add(signature1);
            Signature signature2 = new ParenthesesClose(")", PARENTHESIS, PARENTHESIS + 1);
            signature2.Entry = Add(signature2);
            Signature signature3 = new ParenthesesDelimiter(",", PARENTHESIS, PARENTHESIS + 1);
            signature3.Entry = Add(signature3);
        }

        protected virtual void DoInitLogicalOperators()
        {
            int count = Count;
            Signature signature1 = new Assign(":", count, VistaDBType.Unknown);
            signature1.Entry = Add(signature1);
            Signature signature2 = new Assign(":=", count, VistaDBType.Unknown);
            signature2.Entry = Add(signature2);
            LOGICAL_CONST = Count;
            Signature signature3 = new TrueSignature("TRUE", LOGICAL_CONST);
            signature3.Entry = Add(signature3);
            Signature signature4 = new TrueSignature("YES", LOGICAL_CONST);
            signature4.Entry = Add(signature4);
            Signature signature5 = new FalseSignature("FALSE", LOGICAL_CONST);
            signature5.Entry = Add(signature5);
            Signature signature6 = new FalseSignature("NO", LOGICAL_CONST);
            signature6.Entry = Add(signature6);
            Signature signature7 = new NullSignature("NULL", LOGICAL_CONST);
            signature7.Entry = Add(signature7);
            LOGICAL_BITWISE = Count;
            Signature signature8 = new AndBitwise("AND", LOGICAL_BITWISE);
            signature8.Entry = Add(signature8);
            Signature signature9 = new AndBitwise("&&", LOGICAL_BITWISE);
            signature9.Entry = Add(signature9);
            Signature signature10 = new OrBitwise("OR", LOGICAL_BITWISE);
            signature10.Entry = Add(signature10);
            Signature signature11 = new OrBitwise("||", LOGICAL_BITWISE);
            signature11.Entry = Add(signature11);
            Signature signature12 = new XorBitwise("XOR", LOGICAL_BITWISE);
            signature12.Entry = Add(signature12);
            Signature signature13 = new NotBitwise("NOT", LOGICAL_BITWISE);
            signature13.Entry = Add(signature13);
            Signature signature14 = new NotBitwise("!", LOGICAL_BITWISE);
            signature14.Entry = Add(signature14);
            LOGICAL_COMPARE = Count;
            Signature signature15 = new Equal("=", LOGICAL_COMPARE);
            signature15.Entry = Add(signature15);
            Signature signature16 = new Less("<", LOGICAL_COMPARE);
            signature16.Entry = Add(signature16);
            Signature signature17 = new Great(">", LOGICAL_COMPARE);
            signature17.Entry = Add(signature17);
            Signature signature18 = new LessEqual("<=", LOGICAL_COMPARE);
            signature18.Entry = Add(signature18);
            Signature signature19 = new GreatEqual(">=", LOGICAL_COMPARE);
            signature19.Entry = Add(signature19);
            Signature signature20 = new IsNull("IS", LOGICAL_COMPARE);
            signature20.Entry = Add(signature20);
            Signature signature21 = new ReadOnlySignature(Count, PARENTHESIS + 1);
            signature21.Entry = Add(signature21);
            Signature signature22 = new ForeignKeyConstraint(Count, PARENTHESIS + 1);
            signature22.Entry = Add(signature22);
            Signature signature23 = new NonreferencedPrimaryKey(Count, PARENTHESIS + 1);
            signature23.Entry = Add(signature23);
        }

        protected virtual void DoInitMathOperators()
        {
            Signature signature1 = new Minus("-", Count, 1);
            signature1.Entry = Add(signature1);
            Signature signature2 = new UnaryMinus("-", Count);
            signature2.Entry = Add(signature2);
            Signature signature3 = new Plus("+", Count, 1);
            signature3.Entry = Add(signature3);
            Signature signature4 = new UnaryPlus("+", Count);
            signature4.Entry = Add(signature4);
            Signature signature5 = new Multiplication("*", Count);
            signature5.Entry = Add(signature5);
            Signature signature6 = new Dividing("/", Count);
            signature6.Entry = Add(signature6);
        }

        protected virtual void DoInitLanguageOperators()
        {
            Signature signature1 = new Append(";", Count);
            signature1.Entry = Add(signature1);
            int count1 = Count;
            Signature signature2 = new GetDate("NOW", count1, PARENTHESIS + 1);
            signature2.Entry = Add(signature2);
            Signature signature3 = new GetDate("GETDATE", count1, PARENTHESIS + 1);
            signature3.Entry = Add(signature3);
            Signature signature4 = new GetUtcDate("GETUTCDATE", Count, PARENTHESIS + 1);
            signature4.Entry = Add(signature4);
            Signature signature5 = new Date("DATE", Count, PARENTHESIS + 1);
            signature5.Entry = Add(signature5);
            Signature signature6 = new StringConversion("STRING", Count, PARENTHESIS + 1);
            signature6.Entry = Add(signature6);
            Signature signature7 = new FloatConversion("FLOAT", Count, PARENTHESIS + 1);
            signature7.Entry = Add(signature7);
            Signature signature8 = new DoubleConversion("DOUBLE", Count, PARENTHESIS + 1);
            signature8.Entry = Add(signature8);
            Signature signature9 = new DecimalConversion("DECIMAL", Count, PARENTHESIS + 1);
            signature9.Entry = Add(signature9);
            Signature signature10 = new MoneyConversion("MONEY", Count, PARENTHESIS + 1);
            signature10.Entry = Add(signature10);
            Signature signature11 = new SmallMoneyConversion("SMALLMONEY", Count, PARENTHESIS + 1);
            signature11.Entry = Add(signature11);
            Signature signature12 = new Int64Conversion("LONG", Count, PARENTHESIS + 1);
            signature12.Entry = Add(signature12);
            Signature signature13 = new IdentitySignature(Count, PARENTHESIS + 1);
            signature13.Entry = Add(signature13);
            int count2 = Count;
            Signature signature14 = new GuidSignature("GUID", count2, PARENTHESIS + 1);
            signature14.Entry = Add(signature14);
            Signature signature15 = new GuidSignature("NEWID", count2, PARENTHESIS + 1);
            signature15.Entry = Add(signature15);
        }

        protected virtual void DoInitUDFs()
        {
        }
    }
}
