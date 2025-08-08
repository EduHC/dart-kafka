class OffsetCommitPartitionResponse {
  OffsetCommitPartitionResponse({
    required this.id,
    required this.errorCode,
    this.errorMessage,
  });
  final int id;
  final int errorCode;
  final String? errorMessage;

  @override
  String toString() =>
      'ResOffsetCommitPartition -> id: $id, errorCode: $errorCode, errorMessage: $errorMessage';
}
