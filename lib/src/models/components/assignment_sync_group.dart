import 'package:dart_kafka/dart_kafka.dart';

class AssignmentSyncGroup {
  final String memberId;
  final Assignment? assignment;

  AssignmentSyncGroup({
    required this.memberId,
    this.assignment,
  });

  @override
  String toString() {
    return "AssignmentSyncGroup -> memberId: $memberId, assigment: $assignment";
  }
}
