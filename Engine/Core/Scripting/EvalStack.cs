using System;
using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Scripting
{
    internal class EvalStack
    {
        internal static readonly char SingleQuote = '\'';
        internal static readonly char FloatPunctuation = '.';
        internal static readonly char FloatExponent = 'e';
        internal static readonly char FloatUpperExponent = 'E';
        internal static readonly char FloatMinusSign = '-';
        internal static readonly char FloatPlusSign = '+';
        internal static readonly char DecimalPunctuation = 'm';
        private ProcedureCode pcode;
        private Collector collector;
        private Connection connection;
        private DataStorage activeStorage;
        private char[] expression;
        private SignatureList signatures;
        protected Row rowResult;
        private Row.Column columnResult;

        internal EvalStack(Connection connection, DataStorage activeStorage)
        {
            pcode = new ProcedureCode();
            collector = new Collector();
            this.connection = connection;
            this.activeStorage = activeStorage;
        }

        internal char[] Expression
        {
            get
            {
                return expression;
            }
        }

        internal DataStorage ActiveStorage
        {
            get
            {
                return activeStorage;
            }
        }

        internal Row EvaluatedRow
        {
            get
            {
                return rowResult;
            }
        }

        internal Row.Column EvaluatedColumn
        {
            get
            {
                return columnResult;
            }
        }

        internal bool TrueBooleanValue
        {
            get
            {
                if (columnResult != null && columnResult.Type == VistaDBType.Bit)
                    return (bool)columnResult.Value;
                return false;
            }
        }

        internal VistaDBType EvaluatedType
        {
            get
            {
                return collector[0].Signature.ReturnType;
            }
        }

        internal int CodeLength
        {
            get
            {
                return pcode.Count;
            }
        }

        internal SignatureList Signatures
        {
            set
            {
                signatures = value;
            }
        }

        internal Row.Column FirstColumn
        {
            get
            {
                foreach (PCodeUnit pcodeUnit in (List<PCodeUnit>)pcode)
                {
                    if (pcodeUnit.Signature.Group == signatures.COLUMN)
                        return pcodeUnit.ResultColumn;
                }
                return null;
            }
        }

        internal bool IsConstantResult
        {
            get
            {
                return false;
            }
        }

        private bool GoHead()
        {
            try
            {
                return pcode.GoHead();
            }
            finally
            {
                collector.GoHead();
            }
        }

        private PCodeUnit NextPCode()
        {
            return pcode.PopBack();
        }

        private PCodeUnit PeekPCode()
        {
            return pcode.Peek();
        }

        private int SyncNext()
        {
            PCodeUnit pcodeUnit1 = NextPCode();
            PCodeUnit unit = collector.ActivateNextRegister(pcodeUnit1);
            Signature signature1 = pcodeUnit1.Signature;
            int group = signature1.Group;
            Signature.Operations operation = signature1.Operation;
            int entry = signature1.Entry;
            int activeUnits1 = collector.ActiveUnits;
            int position = activeUnits1;
            if (group == signatures.BREAK)
                return TestBranches(0, activeUnits1 - 1);
            int num1;
            if (operation == Signature.Operations.BgnGroup)
            {
                num1 = 0;
                for (; position >= 0; --position)
                {
                    Signature signature2 = collector[position].Signature;
                    if (signature2.Operation == Signature.Operations.EndGroup && signature2.Entry == signature1.EndOfGroupEntry)
                    {
                        num1 = activeUnits1 - position - 1;
                        collector.ExtractPosition(position);
                        ExtractFirstSignature(signature2.Group, Signature.Operations.EndGroup);
                        if (group == signatures.PARENTHESIS)
                        {
                            ExtractFirstSignature(group, Signature.Operations.BgnGroup);
                            collector.Pop();
                            return collector.ActiveUnits;
                        }
                        activeUnits1 = collector.ActiveUnits;
                        break;
                    }
                }
                if (position < 0)
                    return -286;
                if (num1 - pcodeUnit1.DelimitersCount > 1)
                    return -287;
                if (signature1.NumberFormalOperands != -1)
                {
                    while (num1 != signature1.NumberFormalOperands && signature1.Group == group && signature1.Operation == operation)
                        signature1 = signatures[++entry];
                }
                if (signature1.Group != group || signature1.Operation != operation)
                    return -288;
            }
            else
                num1 = signature1.NumberFormalOperands;
            if (signature1.Operation != Signature.Operations.EndGroup)
            {
                if (num1 > 0)
                {
                    while (signature1.Group == group && signature1.Operation == operation)
                    {
                        num1 = signature1.NumberFormalOperands;
                        bool flag = true;
                        int index = activeUnits1 - num1;
                        if (index < 0)
                        {
                            if (signature1.NumberFormalOperands <= 0)
                                return -285;
                            signature1 = signatures[++entry];
                        }
                        else
                        {
                            int parameterIndex = 0;
                            int offset = index;
                            while (flag && parameterIndex < num1)
                            {
                                PCodeUnit pcodeUnit2 = collector[index];
                                flag = signature1.CompatibleType(parameterIndex, pcodeUnit2.Signature.ReturnType);
                                ++parameterIndex;
                                ++index;
                            }
                            if (flag && parameterIndex == num1)
                            {
                                signature1 = signature1.DoCloneSignature();
                                signature1.FixReturnTypeAndParameters(collector, offset);
                                break;
                            }
                            signature1 = signatures[++entry];
                        }
                    }
                    if (signature1.Group != group || signature1.Operation != operation)
                        return -289;
                }
                if (num1 == 0)
                {
                    while (signature1.Group == group && signature1.Operation == operation && (!(unit.ResultColumn == null) && unit.ResultColumn.InternalType != signature1.ReturnType))
                        signature1 = signatures[++entry];
                    if (signature1.Group != group || signature1.Operation != operation)
                        return -290;
                }
            }
            collector.MovePeekBy(-num1);
            unit.ParametersCount = num1;
            unit.Signature = signature1;
            int num2 = num1;
            for (int activeUnits2 = collector.ActiveUnits; activeUnits2 < collector.ActiveUnits + num1; ++activeUnits2)
                num2 += collector[activeUnits2].Depth;
            unit.Depth = num2;
            pcodeUnit1.CopyFrom(unit);
            collector.Peek().Signature = unit.Signature;
            return collector.ActiveUnits;
        }

        private void ExtractFirstSignature(int groupId, Signature.Operations operation)
        {
            for (int activeUnits = pcode.ActiveUnits; activeUnits >= 0; --activeUnits)
            {
                Signature signature = pcode[activeUnits].Signature;
                if (signature.Group == groupId && signature.Operation == operation)
                {
                    pcode.ExtractPosition(activeUnits);
                    break;
                }
            }
        }

        private int TestBranches(int beginCollectorPosition, int endCollectorPosition)
        {
            return beginCollectorPosition != endCollectorPosition ? -285 : -1;
        }

        private PCodeUnit ExecNext(Row contextRow)
        {
            PCodeUnit pcodeUnit = NextPCode();
            bool bypassNextGoup = false;
            int iterator = pcode.Iterator;
            collector.ExecRegister(pcodeUnit, connection, activeStorage, contextRow, ref bypassNextGoup, rowResult);
            pcode.Iterator = iterator;
            return BypassGroup(pcodeUnit, bypassNextGoup);
        }

        private PCodeUnit BypassGroup(PCodeUnit unit, bool bypassGroup)
        {
            Signature signature1 = unit.Signature;
            bypassGroup = bypassGroup && signature1.Group != signatures.BREAK;
            if (!bypassGroup)
                return unit;
            int bypassToGroup = signature1.BypassToGroup;
            int num = -1;
            Signature signature2;
            do
            {
                unit = NextPCode();
                signature2 = unit.Signature;
                ++num;
            }
            while (signature2.Group != signatures.BREAK && (unit.Depth != num || signature2.Operation != Signature.Operations.BgnGroup || signature2.Group != bypassToGroup));
            return unit;
        }

        internal void Prepare()
        {
            pcode.ClearCode();
            collector.ClearCode();
        }

        internal void SaveExpression(string expression)
        {
            this.expression = new char[expression.Length];
            expression.CopyTo(0, this.expression, 0, this.expression.Length);
        }

        internal void PushCollector(Signature signature)
        {
            collector.Push(new PCodeUnit(signature));
        }

        internal void PushCollector(PCodeUnit unit)
        {
            collector.Push(unit);
        }

        internal PCodeUnit PopCollector()
        {
            return collector.PopAndFree();
        }

        internal PCodeUnit PeekCollector()
        {
            return collector.Peek();
        }

        internal void PushBreak()
        {
            PushCollector(signatures[signatures.BREAK]);
        }

        internal void PushColumn(DataStorage storage, Row.Column sourceColumn)
        {
            PushCollector(signatures[signatures.COLUMN]);
            PCodeUnit pcodeUnit = PeekCollector();
            pcodeUnit.ResultColumn = sourceColumn;
            pcodeUnit.ActiveStorage = storage;
        }

        internal int PushStringConstant(int offset, DataStorage activeStorage)
        {
            int length = Expression.Length;
            char singleQuote = SingleQuote;
            string str = string.Empty;
            int num = 0;
            string val;
            while (true)
            {
                int startIndex = ++offset;
                bool flag = false;
                while (offset < length && (flag = singleQuote.CompareTo(Expression[offset]) != 0))
                    ++offset;
                if (offset < length || !flag)
                {
                    val = str + new string(Expression, startIndex, offset - startIndex);
                    ++offset;
                    if (offset < length && singleQuote.CompareTo(Expression[offset]) == 0)
                    {
                        ++num;
                        str = val + singleQuote;
                    }
                    else
                        goto label_8;
                }
                else
                    break;
            }
            return -1;
        label_8:
            Constant constant = (Constant)signatures[signatures.CONSTANT].DoCloneSignature();
            PushCollector(constant);
            PCodeUnit pcodeUnit = PeekCollector();
            NCharColumn ncharColumn = new NCharColumn(val, 8192, activeStorage.Culture, true, NCharColumn.DefaultUnicode);
            constant.ConstantColumn = ncharColumn;
            pcodeUnit.ResultColumn = ncharColumn;
            pcodeUnit.ActiveStorage = null;
            return val.Length + 2 + num;
        }

        internal int PushNumericConstant(int offset)
        {
            int length1 = Expression.Length;
            int startIndex = offset;
            while (offset < length1 && char.IsNumber(Expression[offset]))
                ++offset;
            bool flag1 = offset < length1 && Expression[offset] == FloatPunctuation;
            if (flag1)
            {
                ++offset;
                while (offset < length1 && char.IsNumber(Expression[offset]))
                    ++offset;
            }
            bool flag2 = offset < length1 && Expression[offset] == DecimalPunctuation;
            bool flag3 = offset < length1 && (Expression[offset] == FloatExponent || Expression[offset] == FloatUpperExponent);
            if (flag3)
            {
                if (++offset < length1 && (Expression[offset] == FloatPlusSign || Expression[offset] == FloatMinusSign))
                    ++offset;
                int num = offset;
                while (offset < length1 && char.IsNumber(Expression[offset]))
                    ++offset;
                if (num == offset)
                    throw new VistaDBException(299, new string(Expression, startIndex, offset));
            }
            else if (flag2)
                ++offset;
            Constant constant = (Constant)signatures[signatures.CONSTANT].DoCloneSignature();
            PushCollector(constant);
            PCodeUnit pcodeUnit = PeekCollector();
            int length2 = offset - startIndex;
            if (flag2)
            {
                string str = new string(Expression, startIndex, length2);
                Decimal val = Decimal.Parse(new string(Expression, startIndex, length2 - 1));
                pcodeUnit.ResultColumn = new DecimalColumn(val);
            }
            else if (flag1 || flag3)
            {
                double val = double.Parse(new string(Expression, startIndex, length2), CrossConversion.NumberFormat);
                pcodeUnit.ResultColumn = new FloatColumn(val);
            }
            else
            {
                long val = long.Parse(new string(Expression, startIndex, length2));
                pcodeUnit.ResultColumn = val <= int.MaxValue ? (val <= short.MaxValue ? new SmallIntColumn((short)val) : (Row.Column)new IntColumn((int)val)) : new BigIntColumn(val);
            }
            constant.ConstantColumn = pcodeUnit.ResultColumn;
            pcodeUnit.ActiveStorage = null;
            return length2;
        }

        internal int PushRunTimeTableContext(int offset, int nameLen)
        {
            return -1;
        }

        internal bool IsEmptyCollector()
        {
            return collector.Empty;
        }

        internal void PushPCode(PCodeUnit unit)
        {
            pcode.Push(unit);
        }

        internal bool IsEmptyPcode()
        {
            return pcode.Empty;
        }

        internal void InsertEndOfGroup(int endOfGroupEntry)
        {
            PushCollector(signatures[endOfGroupEntry]);
            PushPCode(PopCollector());
        }

        internal void InsertElseGroup()
        {
        }

        internal void InsertIfGroupFinalization()
        {
        }

        internal void ReleaseCollector()
        {
            while (PeekCollector().Signature.Group != signatures.BREAK)
                PushPCode(PopCollector());
        }

        internal int SignaturesValidation(bool checkBoolean, ref string expression)
        {
            if (!GoHead())
                return 285;
            int num;
            do
            {
                num = SyncNext();
            }
            while (num >= 0);
            if (num < -1)
            {
                expression = new string(PeekPCode().Signature.Name);
                return -num;
            }
            return checkBoolean && EvaluatedType != VistaDBType.Bit ? 291 : 0;
        }

        internal void Exec(Row contextRow)
        {
            Exec(contextRow, null);
        }

        internal virtual void Exec(Row contextRow, Row targetResult)
        {
            rowResult = targetResult;
            if (rowResult != null)
            {
                rowResult.Clear();
                rowResult.CopyMetaData(contextRow);
            }
            if (!GoHead())
                throw new VistaDBException(308, new string(expression));

            while (ExecNext(contextRow).Signature.Group != signatures.BREAK) ;
            columnResult = collector.ColumnResult;
            if (targetResult == null || targetResult.Count != 0 || !(columnResult != null))
                return;
            targetResult.AppendColumn(columnResult);
        }

        internal List<Row.Column> EnumColumns()
        {
            List<Row.Column> columnList = new List<Row.Column>();
            foreach (PCodeUnit pcodeUnit in (List<PCodeUnit>)pcode)
            {
                if (pcodeUnit.Signature.Group == signatures.COLUMN)
                    columnList.Add(pcodeUnit.ResultColumn);
            }
            return columnList;
        }
    }
}
