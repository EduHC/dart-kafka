class AssignmentTopicMetadata {
  AssignmentTopicMetadata({
    required this.topicName,
    required this.partitions,
  });
  final String topicName;
  final List<int> partitions;

  @override
  String toString() =>
      'AssignmentTopicMetadata -> topic: $topicName, partitions: $partitions';
}
