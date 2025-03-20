class Broker {
  final int? nodeId;
  final String host;
  final int port;
  String? rack;

  Broker({this.nodeId, required this.host, required this.port, this.rack});

  @override
  String toString() {
    return "nodeId: $nodeId, host: $host, port: $port, rack: $rack";
  }
}
