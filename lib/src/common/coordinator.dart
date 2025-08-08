class Coordinator {
  Coordinator({
    required this.key,
    required this.nodeId,
    required this.host,
    required this.port,
    required this.errorCode,
    this.errorMessage,
  });
  final String key;
  final int nodeId;
  final String host;
  final int port;
  final int errorCode;
  final String? errorMessage;

  @override
  String toString() =>
      'Coordinator -> Key: $key, nodeId: $nodeId, host: $host, port: $port, errorCode: $errorCode, errorMessage: $errorMessage';
}
