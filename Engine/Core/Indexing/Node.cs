using System;
using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Indexing
{
    internal class Node : List<Row>, IDisposable
    {
        private KeyPosition keyPosition = KeyPosition.Equal;
        private int keyIndex = -1;
        private BufferBars keyBars = new BufferBars();
        private NodeHeader data;
        private Tree parentTree;
        private Row topKey;
        private Row bottomKey;
        private int level;
        private Row previousAppend;
        private int appendedLength;
        private Row precedenceKey;
        private readonly bool suspect;
        private bool isDisposed;

        internal static Node CreateInstance(ulong nodeId, int levelIndex, Tree parentTree, Row patternKey, bool crypted, int pageSize)
        {
            Node node = new Node(parentTree, patternKey, levelIndex);
            node.data = NodeHeader.CreateInstance(crypted ? Header.HeaderId.INDEX_NODE_CRYPT : Header.HeaderId.INDEX_NODE, node, 0, pageSize);
            node.data.Position = nodeId;
            node.data.AssignBuffer();
            return node;
        }

        private Node(Tree parentTree, Row patternKey, int levelIndex)
        {
            level = levelIndex;
            this.parentTree = parentTree;
            previousAppend = patternKey.CopyInstance();
            InitTopBottom(patternKey);
        }

        internal new Row this[int index]
        {
            get
            {
                if (index < 0)
                    return topKey;
                if (index < Count)
                    return base[index];
                return bottomKey;
            }
        }

        internal ulong Id
        {
            get
            {
                return data.Position;
            }
        }

        internal int KeyIndex
        {
            get
            {
                return keyIndex;
            }
            set
            {
                keyIndex = value;
            }
        }

        internal KeyPosition KeyRank
        {
            get
            {
                return keyPosition;
            }
        }

        internal int KeyCount
        {
            get
            {
                return (int)data.KeyCount;
            }
            set
            {
                data.KeyCount = (uint)value;
            }
        }

        internal int NodeLength
        {
            get
            {
                return data.Size;
            }
        }

        internal int NodeHeaderLength
        {
            get
            {
                return data.DataApartment + data.ExpandingInfo.GetBufferLength((Row.Column)null);
            }
        }

        internal int AppendedLength
        {
            get
            {
                return appendedLength;
            }
            set
            {
                appendedLength = value;
            }
        }

        internal Row PrecedenceKey
        {
            get
            {
                return precedenceKey;
            }
            set
            {
                precedenceKey = value;
            }
        }

        internal int TreeLevel
        {
            get
            {
                return level;
            }
        }

        internal bool Modified
        {
            get
            {
                return data.Modified;
            }
        }

        internal bool SuspectForCorruption
        {
            get
            {
                return suspect;
            }
        }

        internal bool IsDisposed
        {
            get
            {
                return isDisposed;
            }
        }

        internal virtual ulong LeftNodeId
        {
            get
            {
                return data.LeftId;
            }
            set
            {
                data.LeftId = value;
            }
        }

        internal virtual ulong RightNodeId
        {
            get
            {
                return data.RightId;
            }
            set
            {
                data.RightId = value;
            }
        }

        internal virtual NodeType Type
        {
            get
            {
                return data.NodeType;
            }
            set
            {
                data.NodeType = value;
            }
        }

        internal virtual bool IsRoot
        {
            get
            {
                return data.IsRoot;
            }
            set
            {
            }
        }

        internal virtual bool IsLeaf
        {
            get
            {
                return data.IsLeaf;
            }
            set
            {
            }
        }

        internal Node GetLeftNode()
        {
            return parentTree.GetNodeAtPosition(LeftNodeId);
        }

        internal Node GetRightNode()
        {
            return parentTree.GetNodeAtPosition(RightNodeId);
        }

        private void InitTopBottom(Row patternKey)
        {
            topKey = patternKey.CopyInstance();
            bottomKey = patternKey.CopyInstance();
            topKey.InitTop();
            bottomKey.InitBottom();
        }

        private void Decrypt()
        {
            if (!OnDecrypt(parentTree.ParentIndex))
                throw new VistaDBException(331);
        }

        internal bool IsSplitRequired(int dataLen)
        {
            if (KeyCount >= 3)
                return dataLen > NodeLength - NodeHeaderLength;
            return false;
        }

        internal void MoveKeysTo(Node destinationNode)
        {
            MoveRightKeysTo(destinationNode, Count);
        }

        internal void MoveRightKeysTo(Node destinationNode, int count)
        {
            int index1 = Count - count;
            int count1 = Count;
            int dataLength = data.DataLength;
            int num = data.Buffer.Length - dataLength;
            Buffer.BlockCopy((Array)data.Buffer, num, (Array)destinationNode.data.Buffer, num, dataLength);
            destinationNode.data.DataLength = dataLength;
            destinationNode.keyBars.Clear();
            destinationNode.keyBars.ExpandedDataBuffer = (byte[])data.ExpandingInfo.Value;
            int index2 = index1;
            int count2 = Count;
            for (; index2 < count1; ++index2)
            {
                destinationNode.Add(this[index2]);
                destinationNode.keyBars.Add(keyBars[index2]);
            }
            RemoveRange(index1, count);
            keyBars.RemoveRange(index1, count);
            if (destinationNode.keyBars.Count > 0 && index1 > 0)
                destinationNode.keyBars.ResetBar(0);
            KeyCount -= count;
            destinationNode.KeyCount += count;
        }

        internal void SetModified(bool modified)
        {
            data.Modified = modified;
        }

        internal bool PackInformation()
        {
            int num1 = NodeLength - NodeHeaderLength;
            int dataLen = 0;
            int expandedDataLen = 0;
            bool flag1 = false;
            int num2 = 0;
            Row precedenceRow1 = (Row)null;
            int index = 0;
            bool flag2 = false;
            foreach (Row row in (List<Row>)this)
            {
                row.PartialRow = !IsLeaf;
                try
                {
                    BufferBars.ContiguousBar keyBar = keyBars[index];
                    bool flag3 = keyBar.OriginOffset < 0;
                    int num3 = flag3 || flag2 ? row.GetMemoryApartment(precedenceRow1) : keyBar.Apartment;
                    flag2 = flag3;
                    if (flag1)
                    {
                        expandedDataLen += num3;
                    }
                    else
                    {
                        dataLen += num3;
                        if (dataLen > num1)
                        {
                            dataLen -= num3;
                            flag1 = true;
                            expandedDataLen = num3;
                        }
                        else
                            ++num2;
                    }
                }
                finally
                {
                    precedenceRow1 = row;
                    row.PartialRow = false;
                    ++index;
                }
            }
            if (flag1 && KeyCount > 3)
                return false;
            int num4 = 0;
            int num5 = num2;
            try
            {
                if (dataLen == 0 && expandedDataLen == 0)
                    return true;
                keyBars.InitDataCopyBuffers(data);
                ClearExpandingInfo();
                if (expandedDataLen > 0 && !TestReferenceLength(dataLen, expandedDataLen))
                {
                    num2 = 0;
                    num5 = 0;
                    expandedDataLen += dataLen;
                    dataLen = 0;
                }
                int offset = NodeLength - dataLen;
                byte[] numArray = data.Buffer;
                Row precedenceRow2 = (Row)null;
                int barIndex = 0;
                bool flag3 = false;
                bool flag4 = false;
                bool flag5 = parentTree.ParentIndex.Encryption != null;
                foreach (Row row in (List<Row>)this)
                {
                    row.PartialRow = !IsLeaf;
                    try
                    {
                        if (num2 == 0)
                        {
                            numArray = new byte[expandedDataLen];
                            data.ExpandingInfo.Value = (object)numArray;
                            offset = 0;
                            flag4 = true;
                        }
                        --num2;
                        int num3 = offset;
                        BufferBars.ContiguousBar keyBar = keyBars[barIndex];
                        bool flag6 = keyBar.OriginOffset < 0 || flag5;
                        offset = flag3 || flag6 ? row.FormatRowBuffer(numArray, offset, precedenceRow2) : keyBars.CopyBarContent(barIndex, numArray, offset);
                        flag3 = flag6;
                        keyBar.OriginOffset = num3;
                        keyBar.Expanding = flag4;
                        keyBar.Apartment = offset - num3;
                    }
                    finally
                    {
                        precedenceRow2 = row;
                        row.PartialRow = false;
                        ++barIndex;
                    }
                }
                keyBars.ExpandedDataBuffer = (byte[])data.ExpandingInfo.Value;
                num4 = dataLen;
                return true;
            }
            finally
            {
                data.ActualKeyCount = num5;
                data.DataLength = dataLen;
                data.PackedDataLength = dataLen == 0 ? 0 : num4;
            }
        }

        private bool TestReferenceLength(int dataLen, int expandedDataLen)
        {
            return NodeLength - NodeHeaderLength - dataLen - data.ExpandingInfo.TestReferenceLength(expandedDataLen, NodeLength) >= 0;
        }

        internal void UnpackInformation()
        {
            Clear();
            int dataLength = data.DataLength;
            int packedDataLength = data.PackedDataLength;
            if (KeyCount == 0)
                return;
            Index parentIndex = parentTree.ParentIndex;
            int num = 0;
            for (int keyCount = KeyCount; num < keyCount; ++num)
                Add(parentIndex.CurrentRow.CopyInstance());
            byte[] buffer = data.Buffer;
            int offset1 = buffer.Length - dataLength;
            int actualKeyCount = data.ActualKeyCount;
            Row precedenceRow = (Row)null;
            bool allowPostponing = parentIndex.AllowPostponing;
            keyBars.Clear();
            bool expanding = false;
            foreach (Row row in (List<Row>)this)
            {
                row.PartialRow = !IsLeaf;
                try
                {
                    if (actualKeyCount == 0)
                    {
                        buffer = (byte[])data.ExpandingInfo.Value;
                        offset1 = 0;
                        expanding = true;
                    }
                    --actualKeyCount;
                    int offset2 = offset1;
                    offset1 = row.UnformatRowBuffer(buffer, offset1, precedenceRow);
                    row.ReadExtensions((DataStorage)parentIndex.ParentRowset, allowPostponing);
                    keyBars.AppendBar(offset2, offset1 - offset2, expanding);
                }
                finally
                {
                    precedenceRow = row;
                    row.PartialRow = false;
                }
            }
        }

        internal void ClearApartment()
        {
            DataStorage parentIndex = (DataStorage)parentTree.ParentIndex;
            data.ExpandingInfo.FreeSpace(parentIndex);
            parentIndex.SetFreeCluster(Id, 1);
        }

        internal int GoNodeKey(Row key)
        {
            int num1 = 0;
            int num2 = KeyCount;
            long num3;
            do
            {
                keyIndex = (num1 + num2) / 2;
                num3 = (long)(key - this[keyIndex]);
                if (num3 == 0L)
                {
                    keyPosition = KeyPosition.Equal;
                    return keyIndex;
                }
                if (num3 < 0L)
                    num2 = keyIndex;
                else
                    num1 = keyIndex + 1;
            }
            while (num1 < num2);
            if (num2 >= KeyCount && num3 > 0L)
            {
                keyPosition = KeyPosition.OnRight;
                return keyIndex;
            }
            keyIndex = num2;
            if (num2 == 0)
            {
                keyPosition = KeyPosition.OnLeft;
                return 0;
            }
            keyPosition = KeyPosition.Less;
            return keyIndex - 1;
        }

        internal void PropagateNode(Row oldKey, Row newKey)
        {
            if (IsRoot)
                return;
            Node node = parentTree.GoKey(oldKey, this);
            int keyIndex = node.KeyIndex;
            Row newKey1 = node[keyIndex];
            newKey1.Copy(newKey);
            node.keyBars.ResetBar(keyIndex);
            newKey1.SyncPartialRow();
            newKey1.RefPosition = Id;
            node.SetModified(true);
            if (keyIndex != 0)
                return;
            node.PropagateNode(oldKey, newKey1);
        }

        internal Node DeleteKey(int index)
        {
            int num = --KeyCount;
            Row row = this[index];
            try
            {
                if (num == 0)
                {
                    if (IsRoot)
                    {
                        Type |= NodeType.Leaf;
                        keyIndex = index;
                        ClearExpandingInfo();
                        return this;
                    }
                    Node node1 = parentTree.GoKey(this[0], this);
                    Node rightNode = GetRightNode();
                    Node leftNode = GetLeftNode();
                    Node node2 = (Node)null;
                    try
                    {
                        if (rightNode != null)
                        {
                            rightNode.LeftNodeId = leftNode == null ? Row.EmptyReference : leftNode.Id;
                            node2 = rightNode;
                            node2.keyIndex = 0;
                        }
                        if (leftNode != null)
                        {
                            leftNode.RightNodeId = rightNode == null ? Row.EmptyReference : rightNode.Id;
                            if (node2 == null)
                            {
                                node2 = leftNode;
                                node2.keyIndex = leftNode.KeyCount;
                            }
                        }
                    }
                    finally
                    {
                        Node node3 = node1.DeleteKey(node1.keyIndex);
                        if (node2 == null)
                            node2 = node3;
                        ClearApartment();
                        parentTree.RemoveNode(Id);
                    }
                    return node2;
                }
                if (index == 0)
                    PropagateNode(this[0], this[1]);
                keyIndex = index;
                return this;
            }
            finally
            {
                RemoveAt(index);
                keyBars.DropBar(index);
            }
        }

        private void ClearExpandingInfo()
        {
            data.ExpandingInfo.FreeSpace((DataStorage)parentTree.ParentIndex);
            data.ExpandingInfo.Value = (object)null;
        }

        internal void InsertKey(int newIndex, Row newKey, ulong childNodeId)
        {
            Row newKey1 = newKey.CopyInstance();
            if (!IsLeaf)
                newKey1.RefPosition = childNodeId;
            Insert(newIndex, newKey1);
            ++KeyCount;
            keyBars.InsertBar(newIndex);
            if (IsRoot || newIndex > 0)
                return;
            PropagateNode(this[1], newKey1);
        }

        internal void Update()
        {
            data.Activate(Id);
            Decrypt();
            UnpackInformation();
        }

        internal void InitBars()
        {
            keyBars.Clear();
            int index = 0;
            for (int count = Count; index < count; ++index)
                keyBars.InsertBar(index);
        }

        internal bool Flush(bool initBars)
        {
            if (!data.Modified)
                return false;
            if (initBars)
                InitBars();
            while (!PackInformation())
            {
                parentTree.SplitNode(this);
                if (!data.Modified)
                    return true;
            }
            data.Build(Id);
            return false;
        }

        internal Node GetChildNode(int keyIndex)
        {
            if (!IsLeaf)
                return parentTree.GetNodeAtPosition(this[keyIndex].RefPosition);
            return (Node)null;
        }

        protected virtual char[] OnEncrypt(Index parentIndex)
        {
            return (char[])null;
        }

        protected virtual bool OnDecrypt(Index parentIndex)
        {
            return true;
        }

        public new void Clear()
        {
            if (keyBars != null)
                keyBars.Clear();
            base.Clear();
        }

        public void Dispose()
        {
            if (isDisposed)
                return;
            isDisposed = true;
            GC.SuppressFinalize((object)this);
            Clear();
            if (data != null)
                data = (NodeHeader)null;
            if (keyBars != null)
            {
                keyBars.Clear();
                keyBars = (BufferBars)null;
            }
            parentTree = (Tree)null;
            bottomKey = (Row)null;
            topKey = (Row)null;
            previousAppend = (Row)null;
            precedenceKey = (Row)null;
        }

        [Flags]
        internal enum NodeType : uint
        {
            None = 0,
            Root = 1,
            Leaf = 2,
            Encrypted = 4,
            IndexMap = 8,
            LargeObject = 16, // 0x00000010
            System = 32768, // 0x00008000
            Corrupt = 34952, // 0x00008888
        }

        private class NodeHeader : Header
        {
            private int leftNodeIndex;
            private int rightNodeIndex;
            private int dataLengthIndex;
            private int packedDataLengthIndex;
            private int partialKeysIndex;
            private int expandedDataIndex;
            private Node nodeContainer;
            private int pageSize;

            internal static NodeHeader CreateInstance(HeaderId id, Node node, int signature, int pageSize)
            {
                return new NodeHeader(id, node, signature, pageSize);
            }

            private NodeHeader(HeaderId id, Node node, int signature, int pageSize)
              : base((DataStorage)node.parentTree.ParentIndex, id, EmptyReference, signature, pageSize)
            {
                leftNodeIndex = AppendColumn((IColumn)new BigIntColumn((long)EmptyReference));
                rightNodeIndex = AppendColumn((IColumn)new BigIntColumn((long)EmptyReference));
                dataLengthIndex = AppendColumn((IColumn)new SmallIntColumn((short)0));
                packedDataLengthIndex = AppendColumn((IColumn)new SmallIntColumn((short)0));
                partialKeysIndex = AppendColumn((IColumn)new SmallIntColumn((short)0));
                expandedDataIndex = AppendColumn((IColumn)new BlobColumn());
                nodeContainer = node;
                this.pageSize = pageSize;
            }

            internal bool IsLeaf
            {
                get
                {
                    return ((int)Signature & 2) == 2;
                }
                set
                {
                    if (value)
                        Signature |= 2U;
                    else
                        Signature &= 4294967293U;
                }
            }

            internal bool IsRoot
            {
                get
                {
                    return ((int)(byte)Signature & 1) == 1;
                }
                set
                {
                    Signature = value ? Signature | 1U : Signature & 4294967294U;
                }
            }

            internal ulong LeftId
            {
                get
                {
                    return (ulong)(long)this[leftNodeIndex].Value;
                }
                set
                {
                    Modified = (long)LeftId != (long)value;
                    this[leftNodeIndex].Value = (object)(long)value;
                }
            }

            internal ulong RightId
            {
                get
                {
                    return (ulong)(long)this[rightNodeIndex].Value;
                }
                set
                {
                    Modified = (long)RightId != (long)value;
                    this[rightNodeIndex].Value = (object)(long)value;
                }
            }

            internal NodeType NodeType
            {
                get
                {
                    return (NodeType)Signature;
                }
                set
                {
                    Signature = (uint)value;
                }
            }

            internal uint KeyCount
            {
                get
                {
                    return RowCount;
                }
                set
                {
                    RowCount = value;
                }
            }

            internal int ActualKeyCount
            {
                get
                {
                    return (int)(short)this[partialKeysIndex].Value;
                }
                set
                {
                    Modified = ActualKeyCount != value;
                    this[partialKeysIndex].Value = (object)(short)value;
                }
            }

            internal int DataLength
            {
                get
                {
                    return (int)(short)this[dataLengthIndex].Value;
                }
                set
                {
                    Modified = DataLength != value;
                    this[dataLengthIndex].Value = (object)(short)value;
                }
            }

            internal int PackedDataLength
            {
                get
                {
                    return (int)(short)this[packedDataLengthIndex].Value;
                }
                set
                {
                    Modified = PackedDataLength != value;
                    this[packedDataLengthIndex].Value = (object)(short)value;
                }
            }

            internal BlobColumn ExpandingInfo
            {
                get
                {
                    return (BlobColumn)this[expandedDataIndex];
                }
            }

            internal override bool Modified
            {
                get
                {
                    return base.Modified;
                }
                set
                {
                    if (!value || base.Modified)
                        return;
                    base.Modified = true;
                    if (nodeContainer == null || nodeContainer.parentTree == null)
                        return;
                    nodeContainer.parentTree.ModifiedNode(nodeContainer);
                }
            }

            protected override bool ModifiedVersion
            {
                get
                {
                    return base.ModifiedVersion;
                }
                set
                {
                    if (!value || base.ModifiedVersion)
                        return;
                    base.ModifiedVersion = true;
                    if (nodeContainer == null || nodeContainer.parentTree == null)
                        return;
                    nodeContainer.parentTree.ModifiedNode(nodeContainer);
                }
            }

            public override int GetMemoryApartment(Row precedenceRow)
            {
                if (Buffer != null)
                    return Buffer.Length;
                return base.GetMemoryApartment(precedenceRow);
            }

            internal int DataApartment
            {
                get
                {
                    return base.GetMemoryApartment((Row)null);
                }
            }

            protected override void OnAfterRead(int pageSize, bool justVersion)
            {
                base.OnAfterRead(this.pageSize, justVersion);
            }
        }

        private class BufferBars : List<BufferBars.ContiguousBar>
        {
            private byte[] dataCopyBuffer;
            private byte[] nodeExpandingBuffer;
            private int originalOffset;

            internal byte[] ExpandedDataBuffer
            {
                set
                {
                    nodeExpandingBuffer = value;
                }
            }

            internal void AppendBar(int offset, int length, bool expanding)
            {
                Add(new ContiguousBar(offset, length, expanding));
            }

            internal void InsertBar(int index)
            {
                Insert(index, new ContiguousBar(-1, 0, false));
            }

            internal void ResetBar(int index)
            {
                ContiguousBar contiguousBar = this[index];
                contiguousBar.OriginOffset = -1;
                contiguousBar.Apartment = 0;
                contiguousBar.Expanding = false;
            }

            internal void DropBar(int index)
            {
                RemoveAt(index);
                if (index < 0 || index >= Count)
                    return;
                ResetBar(index);
            }

            internal void InitDataCopyBuffers(NodeHeader nodeHeader)
            {
                byte[] buffer = nodeHeader.Buffer;
                originalOffset = buffer.Length - nodeHeader.DataLength;
                dataCopyBuffer = new byte[nodeHeader.DataLength];
                Buffer.BlockCopy((Array)buffer, originalOffset, (Array)dataCopyBuffer, 0, nodeHeader.DataLength);
                if (nodeExpandingBuffer != null)
                    return;
                nodeExpandingBuffer = (byte[])nodeHeader.ExpandingInfo.Value;
            }

            internal int CopyBarContent(int barIndex, byte[] dataBuffer, int offset)
            {
                ContiguousBar contiguousBar = this[barIndex];
                byte[] numArray;
                int srcOffset;
                if (contiguousBar.Expanding)
                {
                    numArray = nodeExpandingBuffer;
                    srcOffset = contiguousBar.OriginOffset;
                }
                else
                {
                    numArray = dataCopyBuffer;
                    srcOffset = contiguousBar.OriginOffset - originalOffset;
                }
                Buffer.BlockCopy((Array)numArray, srcOffset, (Array)dataBuffer, offset, contiguousBar.Apartment);
                return offset + contiguousBar.Apartment;
            }

            internal class ContiguousBar
            {
                internal int OriginOffset;
                internal int Apartment;
                internal bool Expanding;

                internal ContiguousBar(int originOffset, int apartment, bool expanding)
                {
                    OriginOffset = originOffset;
                    Apartment = apartment;
                    Expanding = expanding;
                }
            }
        }

        internal enum KeyPosition
        {
            Less,
            OnLeft,
            OnRight,
            Equal,
        }
    }
}
