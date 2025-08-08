class Broker {
  Broker({required this.host, required this.port, this.nodeId, this.rack});
  final int? nodeId;
  final String host;
  final int port;
  String? rack;

  @override
  String toString() => 'nodeId: $nodeId, host: $host, port: $port, rack: $rack';
}
