class KafkaPartitionMetadata {
  final int errorCode;
  final int partitionId;
  final int leaderId;
  final List<int> replicas; // List of replica broker IDs
  final List<int> isr; // List of in-sync replica broker IDs
  final int? leaderEpoch;

  KafkaPartitionMetadata({
    required this.errorCode,
    required this.partitionId,
    required this.leaderId,
    required this.replicas,
    required this.isr,
    this.leaderEpoch,
  });

  @override
  String toString() {
    return "PartitionMetadata: partitionId: $partitionId, leaderId: $leaderId, errorCode: $errorCode, replicas: $replicas, in-sync replicas: $isr, leaderEpoch: $leaderEpoch";
  }
}
