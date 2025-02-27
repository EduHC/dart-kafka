class KafkaBroker {
  final int nodeId; // Broker ID
  final String host; // Broker host
  final int port; // Broker port

  KafkaBroker({
    required this.nodeId,
    required this.host,
    required this.port,
  });
}
