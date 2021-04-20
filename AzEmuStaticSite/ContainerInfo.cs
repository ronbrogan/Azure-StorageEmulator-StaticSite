using System;

namespace AzEmuStaticSite
{
    public class ContainerInfo
    {
        public string AccountName { get; set; }
        public string ContainerName { get; set; }
        public DateTime LastModificationTime { get; set; }
        public byte[] ServiceMetadata { get; set; }
        public byte[] Metadata { get; set; }
        public Guid? LeaseId { get; set; }
        public int LeaseState { get; set; }
        public long LeaseDuration { get; set; }
        public DateTime? LeaseEndTime { get; set; }
        public bool IsLeaseOp { get; set; }
    }
}
