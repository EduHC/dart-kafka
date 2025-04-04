class AssignmentTopicData {
  final String topicName;
  final List<int> partitions;

  AssignmentTopicData({
    required this.topicName,
    required this.partitions,
  });

  @override
  String toString() {
    return "AssignmentTopicData -> topic: $topicName, partitions: $partitions";
  }
}
