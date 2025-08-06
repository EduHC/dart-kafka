class GroupTopic {
  GroupTopic({
    required this.name,
    required this.partitions,
  });
  final String name;
  final List<int> partitions;

  @override
  String toString() => 'GroupTopic -> name: $name, partitions: $partitions';
}
