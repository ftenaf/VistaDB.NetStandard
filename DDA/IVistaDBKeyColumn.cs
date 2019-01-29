namespace VistaDB.DDA
{
  public interface IVistaDBKeyColumn
  {
    int RowIndex { get; }

    bool Descending { get; }
  }
}
