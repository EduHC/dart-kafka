class HeartbeatResponse {
  final int? throttleTimeMs;
  final int errorCode;
  final String? errorMessage;

  HeartbeatResponse({
    required this.errorCode,
    this.throttleTimeMs,
    this.errorMessage,
  });

  @override
  String toString() {
    return "HeartbeatResponse -> throttleTimeMs: $throttleTimeMs, errorCode: $errorCode, errorMessage: $errorMessage";
  }
}
