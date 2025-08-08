import '../../../dart_kafka.dart';

class Assignment {
  Assignment({
    required this.version,
    required this.topics,
    this.userData,
  });
  final int version;
  final List<AssignmentTopicMetadata> topics;
  final List<int>? userData;

  @override
  String toString() =>
      'Assignment -> version: $version, topics: $topics, userData: $userData';
}
