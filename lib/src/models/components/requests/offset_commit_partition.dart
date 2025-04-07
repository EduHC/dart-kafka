class OffsetCommitPartition {
  final int id;
  final int commitedOffset;
  final int commitedLeaderEpoch;
  final String? commitedMetadata;

  OffsetCommitPartition({
    required this.id,
    required this.commitedOffset,
    required this.commitedLeaderEpoch,
    this.commitedMetadata,
  });

  @override
  String toString() {
    return "OffsetCommitPartition -> id: $id, commitedOffset: $commitedOffset, "
        "commitedLeaderEpoch: $commitedLeaderEpoch, metadata: $commitedMetadata";
  }
}
