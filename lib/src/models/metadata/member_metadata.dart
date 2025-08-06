class MemberMetadata {
  MemberMetadata({
    required this.version,
    required this.topics,
  });
  final int version;
  final List<String> topics;

  @override
  String toString() => 'MemberMetadata -> version: $version, topics: $topics';
}
