class HeartbeatResponse {
  HeartbeatResponse({
    required this.errorCode,
    this.throttleTimeMs,
    this.errorMessage,
  });
  final int? throttleTimeMs;
  final int errorCode;
  final String? errorMessage;

  @override
  String toString() =>
      'HeartbeatResponse -> throttleTimeMs: $throttleTimeMs, errorCode: $errorCode, errorMessage: $errorMessage';
}
