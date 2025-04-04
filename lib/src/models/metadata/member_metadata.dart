class MemberMetadata {
  final int version;
  final List<String> topics;

  MemberMetadata({
    required this.version,
    required this.topics,
  });

  @override
  String toString() {
    return "MemberMetadata -> version: $version, topics: $topics";
  }
}
