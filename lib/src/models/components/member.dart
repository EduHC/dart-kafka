class Member {
  final String memberId;
  final String? groupInstanceId;
  final List<int> metadata;

  Member(
      {required this.memberId, required this.metadata, this.groupInstanceId});

  @override
  String toString() {
    return "Member -> id: $memberId, groupInstanceId: $groupInstanceId, metadata: $metadata";
  }
}
