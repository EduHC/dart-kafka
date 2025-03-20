class KafkaPartitionMetadata {
  final int errorCode;
  final String? errorMessage;
  final int partitionId;
  final int leaderId;
  final List<int> replicas; // List of replica broker IDs
  final List<int> isr; // List of in-sync replica broker IDs
  final List<int> offlineReplicas;
  final int? leaderEpoch;

  KafkaPartitionMetadata({
    required this.errorCode,
    required this.partitionId,
    required this.leaderId,
    required this.replicas,
    required this.isr,
    required this.offlineReplicas,
    this.leaderEpoch,
    this.errorMessage,
  });

  @override
  String toString() {
    return "PartitionMetadata: partitionId: $partitionId, leaderId: $leaderId, errorCode: $errorCode, errorMessage: $errorMessage, replicas: $replicas, in-sync replicas: $isr, offlineReplicas: $offlineReplicas, leaderEpoch: $leaderEpoch";
  }
}
