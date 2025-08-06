class OffsetFetchPartition {
  OffsetFetchPartition({
    required this.id,
    required this.commitedOffset,
    required this.commitedLeaderEpoch,
    required this.errorCode,
    this.errorMessage,
  });
  final int id;
  final int commitedOffset;
  final int commitedLeaderEpoch;
  final int errorCode;
  final String? errorMessage;

  @override
  String toString() =>
      'OffsetFetchPartition -> id: $id, commitedOffset: $commitedOffset, '
      'commitedLeaderEpoch: $commitedLeaderEpoch, errorCode: $errorCode, errorMessage: $errorMessage';
}
