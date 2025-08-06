import '../../../dart_kafka.dart';

class FindGroupCoordinatorResponse {
  FindGroupCoordinatorResponse({
    this.throttleTimeMs,
    this.errorCode,
    this.errorMessage,
    this.nodeId,
    this.host,
    this.port,
    this.coordinators,
  });
  final int? throttleTimeMs;
  final int? errorCode;
  final String? errorMessage;
  final int? nodeId;
  final String? host;
  final int? port;
  final List<Coordinator>? coordinators;

  @override
  String toString() =>
      'FindGroupCoordinatorResponse -> throttleTimeMs: $throttleTimeMs, errorCode: $errorCode, errorMessage: $errorMessage, '
      'nodeId: $nodeId, host: $host, port: $port, coordinators: $coordinators';
}
