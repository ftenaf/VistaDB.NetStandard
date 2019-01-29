using VistaDB.Engine.Internal;
using VistaDB.Engine.SQL;

namespace VistaDB.Provider
{
  public class VistaDBPlanBlock
  {
    private VistaDBPlanBlock.BlockType blockType;
    private VistaDBPlanBlock[] childs;
    private VistaDBPlanBlock.PlanBlockCollection childCollection;

    protected VistaDBPlanBlock(VistaDBPlanBlock.BlockType blockType, VistaDBPlanBlock[] childs)
    {
      this.blockType = blockType;
      this.childs = childs;
      this.childCollection = new VistaDBPlanBlock.PlanBlockCollection(this);
    }

    internal static VistaDBPlanBlock CreateExecutionPlan(IQueryStatement query)
    {
      BaseSelectStatement query1 = query as BaseSelectStatement;
      if (query1 != null)
        return (VistaDBPlanBlock) new VistaDBPlanResultBlock(query, new VistaDBPlanBlock[1]
        {
          VistaDBPlanBlock.CreateQueryBlock(query1)
        });
      if (query.SubQueryCount == 1)
        return (VistaDBPlanBlock) new VistaDBPlanResultBlock(query, (VistaDBPlanBlock[]) null);
      VistaDBPlanBlock[] childs = new VistaDBPlanBlock[query.SubQueryCount];
      int index = 0;
      for (int subQueryCount = query.SubQueryCount; index < subQueryCount; ++index)
        childs[index] = VistaDBPlanBlock.CreateExecutionPlan(query.SubQuery(index));
      return new VistaDBPlanBlock(VistaDBPlanBlock.BlockType.Batch, childs);
    }

    private static VistaDBPlanBlock CreateQueryBlock(BaseSelectStatement query)
    {
      query.Optimize();
      VistaDBPlanBlock vistaDbPlanBlock = VistaDBPlanBlock.CreateRowSetBlock(query.RowSet, query.ConstraintOperations, 0);
      SelectStatement selectStatement = query as SelectStatement;
      if (selectStatement == null)
        return vistaDbPlanBlock;
      if (selectStatement.HasAggregate)
        vistaDbPlanBlock = new VistaDBPlanBlock(VistaDBPlanBlock.BlockType.AggregateStream, new VistaDBPlanBlock[1]
        {
          vistaDbPlanBlock
        });
      if (selectStatement.Distinct)
        vistaDbPlanBlock = new VistaDBPlanBlock(VistaDBPlanBlock.BlockType.DistinctSort, new VistaDBPlanBlock[1]
        {
          vistaDbPlanBlock
        });
      if (selectStatement.IsSetTopCount)
        vistaDbPlanBlock = new VistaDBPlanBlock(VistaDBPlanBlock.BlockType.Top, new VistaDBPlanBlock[1]
        {
          vistaDbPlanBlock
        });
      if (selectStatement.Sorted)
        vistaDbPlanBlock = new VistaDBPlanBlock(VistaDBPlanBlock.BlockType.Sort, new VistaDBPlanBlock[1]
        {
          vistaDbPlanBlock
        });
      if (selectStatement.UnionQuery != null)
      {
        vistaDbPlanBlock = new VistaDBPlanBlock(VistaDBPlanBlock.BlockType.Union, new VistaDBPlanBlock[2]
        {
          vistaDbPlanBlock,
          VistaDBPlanBlock.CreateQueryBlock((BaseSelectStatement) selectStatement.UnionQuery)
        });
        if (selectStatement.UnionAll && !selectStatement.Distinct)
          vistaDbPlanBlock = new VistaDBPlanBlock(VistaDBPlanBlock.BlockType.DistinctSort, new VistaDBPlanBlock[1]
          {
            vistaDbPlanBlock
          });
      }
      return vistaDbPlanBlock;
    }

    private static VistaDBPlanBlock CreateRowSetBlock(IRowSet rowSet, ConstraintOperations optTable, int rowIndex)
    {
      if (rowSet == null)
        return new VistaDBPlanBlock(VistaDBPlanBlock.BlockType.Scalar, (VistaDBPlanBlock[]) null);
      if (rowSet is SourceTable)
        return VistaDBPlanBlock.CreateSourceTableBlock((SourceTable) rowSet, optTable, rowIndex);
      VistaDBPlanBlock.BlockType blockType = rowSet is LeftJoin ? VistaDBPlanBlock.BlockType.LeftJoin : VistaDBPlanBlock.BlockType.InnerJoin;
      Join join = (Join) rowSet;
      VistaDBPlanBlock rowSetBlock1 = VistaDBPlanBlock.CreateRowSetBlock(join.LeftRowSet, optTable, rowIndex);
      VistaDBPlanBlock rowSetBlock2 = VistaDBPlanBlock.CreateRowSetBlock(join.RightRowSet, optTable, rowIndex);
      return new VistaDBPlanBlock(blockType, new VistaDBPlanBlock[2]
      {
        rowSetBlock1,
        rowSetBlock2
      });
    }

    private static VistaDBPlanBlock CreateSourceTableBlock(SourceTable table, ConstraintOperations constraintOperations, int rowIndex)
    {
      if (table is NativeSourceTable)
      {
        string indexName;
        string joinedTable;
        if (constraintOperations == null)
        {
          indexName = (string) null;
          joinedTable = (string) null;
        }
        else
        {
          indexName = constraintOperations.GetIndexName(rowIndex, table.CollectionOrder);
          joinedTable = constraintOperations.GetJoinedTable(rowIndex, table);
        }
        return (VistaDBPlanBlock) new VistaDBPlanTableBlock(table.Alias, indexName, joinedTable);
      }
      if (table is QuerySourceTable)
        return VistaDBPlanBlock.CreateQueryBlock((BaseSelectStatement) ((QuerySourceTable) table).Statement);
      if (table is BaseViewSourceTable)
        return VistaDBPlanBlock.CreateQueryBlock((BaseSelectStatement) ((BaseViewSourceTable) table).Statement);
      return (VistaDBPlanBlock) new VistaDBPlanFunctionBlock(table.Alias);
    }

    public VistaDBPlanBlock.BlockType PlanBlockType
    {
      get
      {
        return this.blockType;
      }
    }

    public VistaDBPlanBlock.PlanBlockCollection Childs
    {
      get
      {
        return this.childCollection;
      }
    }

    public enum BlockType
    {
      Batch,
      Result,
      LeftJoin,
      InnerJoin,
      AggregateStream,
      Table,
      Function,
      Merge,
      Union,
      Sort,
      DistinctSort,
      Top,
      Scalar,
    }

    public sealed class PlanBlockCollection
    {
      private VistaDBPlanBlock parent;

      internal PlanBlockCollection(VistaDBPlanBlock parent)
      {
        this.parent = parent;
      }

      public int Count
      {
        get
        {
          if (this.parent.childs != null)
            return this.parent.childs.Length;
          return 0;
        }
      }

      public VistaDBPlanBlock this[int index]
      {
        get
        {
          return this.parent.childs[index];
        }
      }
    }
  }
}
