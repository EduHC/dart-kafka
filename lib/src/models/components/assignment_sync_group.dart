import '../../../dart_kafka.dart';

class AssignmentSyncGroup {
  AssignmentSyncGroup({
    required this.memberId,
    this.assignment,
  });
  final String memberId;
  final Assignment? assignment;

  @override
  String toString() =>
      'AssignmentSyncGroup -> memberId: $memberId, assigment: $assignment';
}
