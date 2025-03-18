class RecordError {
  final int batchIndex;
  final String? errorMessage;

  RecordError({required this.batchIndex, this.errorMessage});

  @override
  String toString() {
    return "RecordError -> batchIndex: $batchIndex, errorMessage: $errorMessage";
  }
}
