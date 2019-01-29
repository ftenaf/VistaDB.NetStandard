namespace VistaDB.DDA
{
  public interface IVistaDBColumnAttributesDifference
  {
    bool IsRenamed { get; }

    bool IsTypeDiffers { get; }

    bool IsMaxLengthDiffers { get; }

    bool IsOrderDiffers { get; }

    bool IsEncryptedDiffers { get; }

    bool IsPackedDiffers { get; }

    bool IsCodePageDiffers { get; }

    bool IsDescriptionDiffers { get; }

    bool IsCaptionDiffers { get; }

    bool IsNullDiffers { get; }

    bool IsReadOnlyDiffers { get; }
  }
}
