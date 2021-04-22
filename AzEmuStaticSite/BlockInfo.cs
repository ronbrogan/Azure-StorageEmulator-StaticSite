using System;

namespace AzEmuStaticSite
{
    public class BlockInfo
    {
        public string AccountName { get; set; }
        public string ContainerName { get; set; }
        public string BlobName { get; set; }
        public DateTime VersionTimestamp { get; set; }
        public bool IsCommitted { get; set; }
        public string BlockId { get; set; }
        public long Length { get; set; }
        public long StartOffset { get; set; }
        public string FilePath { get; set; }
    }
}
