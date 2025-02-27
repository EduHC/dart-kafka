class KafkaPartitionMetadata {
  final int errorCode;
  final int partitionId;
  final int leaderId;
  final List<int> replicas; // List of replica broker IDs
  final List<int> isr; // List of in-sync replica broker IDs

  KafkaPartitionMetadata({
    required this.errorCode,
    required this.partitionId,
    required this.leaderId,
    required this.replicas,
    required this.isr,
  });
}
