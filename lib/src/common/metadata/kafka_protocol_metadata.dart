class ProtocolMetadata {
  ProtocolMetadata({
    required this.version,
    required this.topics,
    this.userDataBytes,
  });
  final int version;
  final List<String> topics;
  final List<int>? userDataBytes;

  @override
  String toString() =>
      'ProtocolMetadata -> version: $version, topics: $topics, userDataBytes: $userDataBytes';
}
