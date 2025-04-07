class GroupTopic {
  final String name;
  final List<int> partitions;

  GroupTopic({
    required this.name,
    required this.partitions,
  });

  @override
  String toString() {
    return "GroupTopic -> name: $name, partitions: $partitions";
  }
}
