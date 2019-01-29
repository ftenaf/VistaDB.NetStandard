using System;
using System.Collections.Generic;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.Core.Indexing
{
  internal class Node : List<Row>, IDisposable
  {
    private Node.KeyPosition keyPosition = Node.KeyPosition.Equal;
    private int keyIndex = -1;
    private Node.BufferBars keyBars = new Node.BufferBars();
    private const int minKeyCount = 3;
    private Node.NodeHeader data;
    private Tree parentTree;
    private Row topKey;
    private Row bottomKey;
    private int level;
    private Row previousAppend;
    private int appendedLength;
    private Row precedenceKey;
    private bool suspect;
    private bool isDisposed;

    internal static Node CreateInstance(ulong nodeId, int levelIndex, Tree parentTree, Row patternKey, bool crypted, int pageSize)
    {
      Node node = new Node(parentTree, patternKey, levelIndex);
      node.data = Node.NodeHeader.CreateInstance(crypted ? Header.HeaderId.INDEX_NODE_CRYPT : Header.HeaderId.INDEX_NODE, node, 0, pageSize);
      node.data.Position = nodeId;
      node.data.AssignBuffer();
      return node;
    }

    private Node(Tree parentTree, Row patternKey, int levelIndex)
    {
      this.level = levelIndex;
      this.parentTree = parentTree;
      this.previousAppend = patternKey.CopyInstance();
      this.InitTopBottom(patternKey);
    }

    internal new Row this[int index]
    {
      get
      {
        if (index < 0)
          return this.topKey;
        if (index < this.Count)
          return base[index];
        return this.bottomKey;
      }
    }

    internal ulong Id
    {
      get
      {
        return this.data.Position;
      }
    }

    internal int KeyIndex
    {
      get
      {
        return this.keyIndex;
      }
      set
      {
        this.keyIndex = value;
      }
    }

    internal Node.KeyPosition KeyRank
    {
      get
      {
        return this.keyPosition;
      }
    }

    internal int KeyCount
    {
      get
      {
        return (int) this.data.KeyCount;
      }
      set
      {
        this.data.KeyCount = (uint) value;
      }
    }

    internal int NodeLength
    {
      get
      {
        return this.data.Size;
      }
    }

    internal int NodeHeaderLength
    {
      get
      {
        return this.data.DataApartment + this.data.ExpandingInfo.GetBufferLength((Row.Column) null);
      }
    }

    internal int AppendedLength
    {
      get
      {
        return this.appendedLength;
      }
      set
      {
        this.appendedLength = value;
      }
    }

    internal Row PrecedenceKey
    {
      get
      {
        return this.precedenceKey;
      }
      set
      {
        this.precedenceKey = value;
      }
    }

    internal int TreeLevel
    {
      get
      {
        return this.level;
      }
    }

    internal bool Modified
    {
      get
      {
        return this.data.Modified;
      }
    }

    internal bool SuspectForCorruption
    {
      get
      {
        return this.suspect;
      }
    }

    internal bool IsDisposed
    {
      get
      {
        return this.isDisposed;
      }
    }

    internal virtual ulong LeftNodeId
    {
      get
      {
        return this.data.LeftId;
      }
      set
      {
        this.data.LeftId = value;
      }
    }

    internal virtual ulong RightNodeId
    {
      get
      {
        return this.data.RightId;
      }
      set
      {
        this.data.RightId = value;
      }
    }

    internal virtual Node.NodeType Type
    {
      get
      {
        return this.data.NodeType;
      }
      set
      {
        this.data.NodeType = value;
      }
    }

    internal virtual bool IsRoot
    {
      get
      {
        return this.data.IsRoot;
      }
      set
      {
      }
    }

    internal virtual bool IsLeaf
    {
      get
      {
        return this.data.IsLeaf;
      }
      set
      {
      }
    }

    internal Node GetLeftNode()
    {
      return this.parentTree.GetNodeAtPosition(this.LeftNodeId);
    }

    internal Node GetRightNode()
    {
      return this.parentTree.GetNodeAtPosition(this.RightNodeId);
    }

    private void InitTopBottom(Row patternKey)
    {
      this.topKey = patternKey.CopyInstance();
      this.bottomKey = patternKey.CopyInstance();
      this.topKey.InitTop();
      this.bottomKey.InitBottom();
    }

    private char[] Encrypt()
    {
      char[] chArray = this.OnEncrypt(this.parentTree.ParentIndex);
      if (chArray == null)
        throw new VistaDBException(330);
      return chArray;
    }

    private void Decrypt()
    {
      if (!this.OnDecrypt(this.parentTree.ParentIndex))
        throw new VistaDBException(331);
    }

    internal bool IsSplitRequired(int dataLen)
    {
      if (this.KeyCount >= 3)
        return dataLen > this.NodeLength - this.NodeHeaderLength;
      return false;
    }

    internal void MoveKeysTo(Node destinationNode)
    {
      this.MoveRightKeysTo(destinationNode, this.Count);
    }

    internal void MoveRightKeysTo(Node destinationNode, int count)
    {
      int index1 = this.Count - count;
      int count1 = this.Count;
      int dataLength = this.data.DataLength;
      int num = this.data.Buffer.Length - dataLength;
      Buffer.BlockCopy((Array) this.data.Buffer, num, (Array) destinationNode.data.Buffer, num, dataLength);
      destinationNode.data.DataLength = dataLength;
      destinationNode.keyBars.Clear();
      destinationNode.keyBars.ExpandedDataBuffer = (byte[]) this.data.ExpandingInfo.Value;
      int index2 = index1;
      int count2 = this.Count;
      for (; index2 < count1; ++index2)
      {
        destinationNode.Add(this[index2]);
        destinationNode.keyBars.Add(this.keyBars[index2]);
      }
      this.RemoveRange(index1, count);
      this.keyBars.RemoveRange(index1, count);
      if (destinationNode.keyBars.Count > 0 && index1 > 0)
        destinationNode.keyBars.ResetBar(0);
      this.KeyCount -= count;
      destinationNode.KeyCount += count;
    }

    internal void SetModified(bool modified)
    {
      this.data.Modified = modified;
    }

    internal bool PackInformation()
    {
      int num1 = this.NodeLength - this.NodeHeaderLength;
      int dataLen = 0;
      int expandedDataLen = 0;
      bool flag1 = false;
      int num2 = 0;
      Row precedenceRow1 = (Row) null;
      int index = 0;
      bool flag2 = false;
      foreach (Row row in (List<Row>) this)
      {
        row.PartialRow = !this.IsLeaf;
        try
        {
          Node.BufferBars.ContiguousBar keyBar = this.keyBars[index];
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
      if (flag1 && this.KeyCount > 3)
        return false;
      int num4 = 0;
      int num5 = num2;
      try
      {
        if (dataLen == 0 && expandedDataLen == 0)
          return true;
        this.keyBars.InitDataCopyBuffers(this.data);
        this.ClearExpandingInfo();
        if (expandedDataLen > 0 && !this.TestReferenceLength(dataLen, expandedDataLen))
        {
          num2 = 0;
          num5 = 0;
          expandedDataLen += dataLen;
          dataLen = 0;
        }
        int offset = this.NodeLength - dataLen;
        byte[] numArray = this.data.Buffer;
        Row precedenceRow2 = (Row) null;
        int barIndex = 0;
        bool flag3 = false;
        bool flag4 = false;
        bool flag5 = this.parentTree.ParentIndex.Encryption != null;
        foreach (Row row in (List<Row>) this)
        {
          row.PartialRow = !this.IsLeaf;
          try
          {
            if (num2 == 0)
            {
              numArray = new byte[expandedDataLen];
              this.data.ExpandingInfo.Value = (object) numArray;
              offset = 0;
              flag4 = true;
            }
            --num2;
            int num3 = offset;
            Node.BufferBars.ContiguousBar keyBar = this.keyBars[barIndex];
            bool flag6 = keyBar.OriginOffset < 0 || flag5;
            offset = flag3 || flag6 ? row.FormatRowBuffer(numArray, offset, precedenceRow2) : this.keyBars.CopyBarContent(barIndex, numArray, offset);
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
        this.keyBars.ExpandedDataBuffer = (byte[]) this.data.ExpandingInfo.Value;
        num4 = dataLen;
        return true;
      }
      finally
      {
        this.data.ActualKeyCount = num5;
        this.data.DataLength = dataLen;
        this.data.PackedDataLength = dataLen == 0 ? 0 : num4;
      }
    }

    private bool TestReferenceLength(int dataLen, int expandedDataLen)
    {
      return this.NodeLength - this.NodeHeaderLength - dataLen - this.data.ExpandingInfo.TestReferenceLength(expandedDataLen, this.NodeLength) >= 0;
    }

    internal void UnpackInformation()
    {
      this.Clear();
      int dataLength = this.data.DataLength;
      int packedDataLength = this.data.PackedDataLength;
      if (this.KeyCount == 0)
        return;
      Index parentIndex = this.parentTree.ParentIndex;
      int num = 0;
      for (int keyCount = this.KeyCount; num < keyCount; ++num)
        this.Add(parentIndex.CurrentRow.CopyInstance());
      byte[] buffer = this.data.Buffer;
      int offset1 = buffer.Length - dataLength;
      int actualKeyCount = this.data.ActualKeyCount;
      Row precedenceRow = (Row) null;
      bool allowPostponing = parentIndex.AllowPostponing;
      this.keyBars.Clear();
      bool expanding = false;
      foreach (Row row in (List<Row>) this)
      {
        row.PartialRow = !this.IsLeaf;
        try
        {
          if (actualKeyCount == 0)
          {
            buffer = (byte[]) this.data.ExpandingInfo.Value;
            offset1 = 0;
            expanding = true;
          }
          --actualKeyCount;
          int offset2 = offset1;
          offset1 = row.UnformatRowBuffer(buffer, offset1, precedenceRow);
          row.ReadExtensions((DataStorage) parentIndex.ParentRowset, allowPostponing);
          this.keyBars.AppendBar(offset2, offset1 - offset2, expanding);
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
      DataStorage parentIndex = (DataStorage) this.parentTree.ParentIndex;
      this.data.ExpandingInfo.FreeSpace(parentIndex);
      parentIndex.SetFreeCluster(this.Id, 1);
    }

    internal int GoNodeKey(Row key)
    {
      int num1 = 0;
      int num2 = this.KeyCount;
      long num3;
      do
      {
        this.keyIndex = (num1 + num2) / 2;
        num3 = (long) (key - this[this.keyIndex]);
        if (num3 == 0L)
        {
          this.keyPosition = Node.KeyPosition.Equal;
          return this.keyIndex;
        }
        if (num3 < 0L)
          num2 = this.keyIndex;
        else
          num1 = this.keyIndex + 1;
      }
      while (num1 < num2);
      if (num2 >= this.KeyCount && num3 > 0L)
      {
        this.keyPosition = Node.KeyPosition.OnRight;
        return this.keyIndex;
      }
      this.keyIndex = num2;
      if (num2 == 0)
      {
        this.keyPosition = Node.KeyPosition.OnLeft;
        return 0;
      }
      this.keyPosition = Node.KeyPosition.Less;
      return this.keyIndex - 1;
    }

    internal void PropagateNode(Row oldKey, Row newKey)
    {
      if (this.IsRoot)
        return;
      Node node = this.parentTree.GoKey(oldKey, this);
      int keyIndex = node.KeyIndex;
      Row newKey1 = node[keyIndex];
      newKey1.Copy(newKey);
      node.keyBars.ResetBar(keyIndex);
      newKey1.SyncPartialRow();
      newKey1.RefPosition = this.Id;
      node.SetModified(true);
      if (keyIndex != 0)
        return;
      node.PropagateNode(oldKey, newKey1);
    }

    internal Node DeleteKey(int index)
    {
      int num = --this.KeyCount;
      Row row = this[index];
      try
      {
        if (num == 0)
        {
          if (this.IsRoot)
          {
            this.Type |= Node.NodeType.Leaf;
            this.keyIndex = index;
            this.ClearExpandingInfo();
            return this;
          }
          Node node1 = this.parentTree.GoKey(this[0], this);
          Node rightNode = this.GetRightNode();
          Node leftNode = this.GetLeftNode();
          Node node2 = (Node) null;
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
            this.ClearApartment();
            this.parentTree.RemoveNode(this.Id);
          }
          return node2;
        }
        if (index == 0)
          this.PropagateNode(this[0], this[1]);
        this.keyIndex = index;
        return this;
      }
      finally
      {
        this.RemoveAt(index);
        this.keyBars.DropBar(index);
      }
    }

    private void ClearExpandingInfo()
    {
      this.data.ExpandingInfo.FreeSpace((DataStorage) this.parentTree.ParentIndex);
      this.data.ExpandingInfo.Value = (object) null;
    }

    internal void InsertKey(int newIndex, Row newKey, ulong childNodeId)
    {
      Row newKey1 = newKey.CopyInstance();
      if (!this.IsLeaf)
        newKey1.RefPosition = childNodeId;
      this.Insert(newIndex, newKey1);
      ++this.KeyCount;
      this.keyBars.InsertBar(newIndex);
      if (this.IsRoot || newIndex > 0)
        return;
      this.PropagateNode(this[1], newKey1);
    }

    internal void Update()
    {
      this.data.Activate(this.Id);
      this.Decrypt();
      this.UnpackInformation();
    }

    internal void InitBars()
    {
      this.keyBars.Clear();
      int index = 0;
      for (int count = this.Count; index < count; ++index)
        this.keyBars.InsertBar(index);
    }

    internal bool Flush(bool initBars)
    {
      if (!this.data.Modified)
        return false;
      if (initBars)
        this.InitBars();
      while (!this.PackInformation())
      {
        this.parentTree.SplitNode(this);
        if (!this.data.Modified)
          return true;
      }
      this.data.Build(this.Id);
      return false;
    }

    internal Node GetChildNode(int keyIndex)
    {
      if (!this.IsLeaf)
        return this.parentTree.GetNodeAtPosition(this[keyIndex].RefPosition);
      return (Node) null;
    }

    protected virtual char[] OnEncrypt(Index parentIndex)
    {
      return (char[]) null;
    }

    protected virtual bool OnDecrypt(Index parentIndex)
    {
      return true;
    }

    public new void Clear()
    {
      if (this.keyBars != null)
        this.keyBars.Clear();
      base.Clear();
    }

    public void Dispose()
    {
      if (this.isDisposed)
        return;
      this.isDisposed = true;
      GC.SuppressFinalize((object) this);
      this.Clear();
      if (this.data != null)
        this.data = (Node.NodeHeader) null;
      if (this.keyBars != null)
      {
        this.keyBars.Clear();
        this.keyBars = (Node.BufferBars) null;
      }
      this.parentTree = (Tree) null;
      this.bottomKey = (Row) null;
      this.topKey = (Row) null;
      this.previousAppend = (Row) null;
      this.precedenceKey = (Row) null;
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

      internal static Node.NodeHeader CreateInstance(Header.HeaderId id, Node node, int signature, int pageSize)
      {
        return new Node.NodeHeader(id, node, signature, pageSize);
      }

      private NodeHeader(Header.HeaderId id, Node node, int signature, int pageSize)
        : base((DataStorage) node.parentTree.ParentIndex, id, Row.EmptyReference, signature, pageSize)
      {
        this.leftNodeIndex = this.AppendColumn((IColumn) new BigIntColumn((long) Row.EmptyReference));
        this.rightNodeIndex = this.AppendColumn((IColumn) new BigIntColumn((long) Row.EmptyReference));
        this.dataLengthIndex = this.AppendColumn((IColumn) new SmallIntColumn((short) 0));
        this.packedDataLengthIndex = this.AppendColumn((IColumn) new SmallIntColumn((short) 0));
        this.partialKeysIndex = this.AppendColumn((IColumn) new SmallIntColumn((short) 0));
        this.expandedDataIndex = this.AppendColumn((IColumn) new BlobColumn());
        this.nodeContainer = node;
        this.pageSize = pageSize;
      }

      internal bool IsLeaf
      {
        get
        {
          return ((int) this.Signature & 2) == 2;
        }
        set
        {
          if (value)
            this.Signature |= 2U;
          else
            this.Signature &= 4294967293U;
        }
      }

      internal bool IsRoot
      {
        get
        {
          return ((int) (byte) this.Signature & 1) == 1;
        }
        set
        {
          this.Signature = value ? this.Signature | 1U : this.Signature & 4294967294U;
        }
      }

      internal ulong LeftId
      {
        get
        {
          return (ulong) (long) this[this.leftNodeIndex].Value;
        }
        set
        {
          this.Modified = (long) this.LeftId != (long) value;
          this[this.leftNodeIndex].Value = (object) (long) value;
        }
      }

      internal ulong RightId
      {
        get
        {
          return (ulong) (long) this[this.rightNodeIndex].Value;
        }
        set
        {
          this.Modified = (long) this.RightId != (long) value;
          this[this.rightNodeIndex].Value = (object) (long) value;
        }
      }

      internal Node.NodeType NodeType
      {
        get
        {
          return (Node.NodeType) this.Signature;
        }
        set
        {
          this.Signature = (uint) value;
        }
      }

      internal uint KeyCount
      {
        get
        {
          return this.RowCount;
        }
        set
        {
          this.RowCount = value;
        }
      }

      internal int ActualKeyCount
      {
        get
        {
          return (int) (short) this[this.partialKeysIndex].Value;
        }
        set
        {
          this.Modified = this.ActualKeyCount != value;
          this[this.partialKeysIndex].Value = (object) (short) value;
        }
      }

      internal int DataLength
      {
        get
        {
          return (int) (short) this[this.dataLengthIndex].Value;
        }
        set
        {
          this.Modified = this.DataLength != value;
          this[this.dataLengthIndex].Value = (object) (short) value;
        }
      }

      internal int PackedDataLength
      {
        get
        {
          return (int) (short) this[this.packedDataLengthIndex].Value;
        }
        set
        {
          this.Modified = this.PackedDataLength != value;
          this[this.packedDataLengthIndex].Value = (object) (short) value;
        }
      }

      internal BlobColumn ExpandingInfo
      {
        get
        {
          return (BlobColumn) this[this.expandedDataIndex];
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
          if (this.nodeContainer == null || this.nodeContainer.parentTree == null)
            return;
          this.nodeContainer.parentTree.ModifiedNode(this.nodeContainer);
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
          if (this.nodeContainer == null || this.nodeContainer.parentTree == null)
            return;
          this.nodeContainer.parentTree.ModifiedNode(this.nodeContainer);
        }
      }

      public override int GetMemoryApartment(Row precedenceRow)
      {
        if (this.Buffer != null)
          return this.Buffer.Length;
        return base.GetMemoryApartment(precedenceRow);
      }

      internal int DataApartment
      {
        get
        {
          return base.GetMemoryApartment((Row) null);
        }
      }

      protected override void OnAfterRead(int pageSize, bool justVersion)
      {
        base.OnAfterRead(this.pageSize, justVersion);
      }
    }

    private class BufferBars : List<Node.BufferBars.ContiguousBar>
    {
      private byte[] dataCopyBuffer;
      private byte[] nodeExpandingBuffer;
      private int originalOffset;

      internal byte[] ExpandedDataBuffer
      {
        set
        {
          this.nodeExpandingBuffer = value;
        }
      }

      internal void AppendBar(int offset, int length, bool expanding)
      {
        this.Add(new Node.BufferBars.ContiguousBar(offset, length, expanding));
      }

      internal void InsertBar(int index)
      {
        this.Insert(index, new Node.BufferBars.ContiguousBar(-1, 0, false));
      }

      internal void ResetBar(int index)
      {
        Node.BufferBars.ContiguousBar contiguousBar = this[index];
        contiguousBar.OriginOffset = -1;
        contiguousBar.Apartment = 0;
        contiguousBar.Expanding = false;
      }

      internal void DropBar(int index)
      {
        this.RemoveAt(index);
        if (index < 0 || index >= this.Count)
          return;
        this.ResetBar(index);
      }

      internal void InitDataCopyBuffers(Node.NodeHeader nodeHeader)
      {
        byte[] buffer = nodeHeader.Buffer;
        this.originalOffset = buffer.Length - nodeHeader.DataLength;
        this.dataCopyBuffer = new byte[nodeHeader.DataLength];
        Buffer.BlockCopy((Array) buffer, this.originalOffset, (Array) this.dataCopyBuffer, 0, nodeHeader.DataLength);
        if (this.nodeExpandingBuffer != null)
          return;
        this.nodeExpandingBuffer = (byte[]) nodeHeader.ExpandingInfo.Value;
      }

      internal int CopyBarContent(int barIndex, byte[] dataBuffer, int offset)
      {
        Node.BufferBars.ContiguousBar contiguousBar = this[barIndex];
        byte[] numArray;
        int srcOffset;
        if (contiguousBar.Expanding)
        {
          numArray = this.nodeExpandingBuffer;
          srcOffset = contiguousBar.OriginOffset;
        }
        else
        {
          numArray = this.dataCopyBuffer;
          srcOffset = contiguousBar.OriginOffset - this.originalOffset;
        }
        Buffer.BlockCopy((Array) numArray, srcOffset, (Array) dataBuffer, offset, contiguousBar.Apartment);
        return offset + contiguousBar.Apartment;
      }

      internal class ContiguousBar
      {
        internal int OriginOffset;
        internal int Apartment;
        internal bool Expanding;

        internal ContiguousBar(int originOffset, int apartment, bool expanding)
        {
          this.OriginOffset = originOffset;
          this.Apartment = apartment;
          this.Expanding = expanding;
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
