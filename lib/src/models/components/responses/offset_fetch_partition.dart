class OffsetFetchPartition {
  final int id;
  final int commitedOffset;
  final int commitedLeaderEpoch;
  final int errorCode;
  final String? errorMessage;

  OffsetFetchPartition({
    required this.id,
    required this.commitedOffset,
    required this.commitedLeaderEpoch,
    required this.errorCode,
    this.errorMessage,
  });

  @override
  String toString() {
    return "OffsetFetchPartition -> id: $id, commitedOffset: $commitedOffset, "
        "commitedLeaderEpoch: $commitedLeaderEpoch, errorCode: $errorCode, errorMessage: $errorMessage";
  }
}
