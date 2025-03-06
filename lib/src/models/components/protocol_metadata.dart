class ProtocolMetadata {
  final int version;
  final List<String> topics;
  final List<int>? userDataBytes;

  ProtocolMetadata(
      {required this.version,
      required this.topics,
      this.userDataBytes});

  @override
  String toString() {
    return "ProtocolMetadata -> version: $version, topics: $topics, userDataBytes: $userDataBytes";
  }
}
