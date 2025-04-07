class ResOffsetCommitPartition {
  final int id;
  final int errorCode;
  final String? errorMessage;

  ResOffsetCommitPartition({
    required this.id,
    required this.errorCode,
    this.errorMessage,
  });

  @override
  String toString() {
    return "ResOffsetCommitPartition -> id: $id, errorCode: $errorCode, errorMessage: $errorMessage";
  }
}
