class AssignmentTopicMetadata {
  final String topicName;
  final List<int> partitions;

  AssignmentTopicMetadata({
    required this.topicName,
    required this.partitions,
  });

  @override
  String toString() {
    return "AssignmentTopicMetadata -> topic: $topicName, partitions: $partitions";
  }
}
