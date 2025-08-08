import '../../dart_kafka.dart';

const name = 'RoundRobinAssigner';
const version = 1;

class Assigner {
  factory Assigner() {
    _instance ??= Assigner._();
    return _instance!;
  }

  Assigner._();
  static Assigner? _instance;

  static List<Protocol> protocol({required List<String> topics}) {
    final Protocol protocol = Protocol(
      name: name,
      metadata: ProtocolMetadata(
        version: version,
        topics: topics,
      ),
    );

    return [protocol];
  }

  /// Method that handles the Load Balance assigning the partitions of topics to all the Members
  /// Initially it'll only append the Partition 0
  static List<AssignmentSyncGroup> assign({
    required List<Member> members,
    required bool isLeader,
  }) =>
      // TODO(Eduardo): Ajustar para um algorítimo estilo RoundRobin e/ ou mais de um.
      members.map(
        (member) {
          if (member.metadata == null) {
            throw Exception(
              'Trying to Assign a partition with empty metadata!',
            );
          }

          return AssignmentSyncGroup(
            memberId: member.memberId,
            assignment: isLeader
                ? Assignment(
                    version: member.metadata!.version,
                    topics: member.metadata!.topics
                        .map(
                          (topic) => AssignmentTopicMetadata(
                            topicName: topic,
                            partitions: [0],
                          ),
                        )
                        .toList(),
                  )
                : null,
          );
        },
      ).toList();
}
