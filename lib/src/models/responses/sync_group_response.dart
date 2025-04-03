class SyncGroupResponse {
  final int? throttleTimeMs;
  final int errorCode;
  final String? errorMessage;
  final String? protocolType;
  final String? protocolName;
  final List<int> assignment;

  SyncGroupResponse({
    this.throttleTimeMs,
    required this.errorCode,
    this.errorMessage,
    this.protocolType,
    this.protocolName,
    required this.assignment,
  });

  @override
  String toString() {
    return "SyncGroupResponse -> throttleTimeMs: $throttleTimeMs, errorCode: $errorCode, errorMessage: $errorMessage, "
        "protocolType: $protocolType, protocolName: $protocolName, assignment: $assignment";
  }
}
