namespace VistaDB.DDA
{
  public enum DDAEventDelegateType
  {
    BeforeInsert = 1,
    BeforeUpdate = 2,
    BeforeDelete = 4,
    AfterInsert = 17, // 0x00000011
    AfterUpdate = 18, // 0x00000012
    AfterDelete = 20, // 0x00000014
    NewVersion = 32, // 0x00000020
  }
}
