class OffsetCommitPartition {
  OffsetCommitPartition({
    required this.id,
    required this.commitedOffset,
    required this.commitedLeaderEpoch,
    this.commitedMetadata,
  });
  final int id;
  final int commitedOffset;
  final int commitedLeaderEpoch;
  final String? commitedMetadata;

  @override
  String toString() =>
      'OffsetCommitPartition -> id: $id, commitedOffset: $commitedOffset, '
      'commitedLeaderEpoch: $commitedLeaderEpoch, metadata: $commitedMetadata';
}
