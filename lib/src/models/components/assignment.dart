import 'package:dart_kafka/dart_kafka.dart';

class Assignment {
  final int version;
  final List<AssignmentTopicData> topics;
  final List<int>? userData;

  Assignment({
    required this.version,
    required this.topics,
    this.userData,
  });

  @override
  String toString() {
    return "Assignment -> version: $version, topics: $topics, userData: $userData";
  }
}
