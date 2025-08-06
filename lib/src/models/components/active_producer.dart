// ignore_for_file: public_member_api_docs, sort_constructors_first
import 'dart:convert';

class ActiveProducer {
  final int id;
  final int epoch;
  final int lastSequence;
  final int lastTimestamp;
  final int coordinatorEpoch;
  final int currentTxnStartOffset;

  ActiveProducer({
    required this.id,
    required this.epoch,
    required this.lastSequence,
    required this.lastTimestamp,
    required this.coordinatorEpoch,
    required this.currentTxnStartOffset,
  });

  @override
  String toString() =>
      'ActiveProducer -> id: $id, epoch: $epoch, lastSequence: $lastSequence, '
      'lastTimestamp: $lastTimestamp, coordinatorEpoch: $coordinatorEpoch, '
      'currentTxnStartOffset: $currentTxnStartOffset';

  Map<String, dynamic> toMap() => <String, dynamic>{
        'id': id,
        'epoch': epoch,
        'lastSequence': lastSequence,
        'lastTimestamp': lastTimestamp,
        'coordinatorEpoch': coordinatorEpoch,
        'currentTxnStartOffset': currentTxnStartOffset,
      };

  factory ActiveProducer.fromMap(Map<String, dynamic> map) => ActiveProducer(
        id: map['id'] as int,
        epoch: map['epoch'] as int,
        lastSequence: map['lastSequence'] as int,
        lastTimestamp: map['lastTimestamp'] as int,
        coordinatorEpoch: map['coordinatorEpoch'] as int,
        currentTxnStartOffset: map['currentTxnStartOffset'] as int,
      );

  String toJson() => json.encode(toMap());

  factory ActiveProducer.fromJson(String source) =>
      ActiveProducer.fromMap(json.decode(source) as Map<String, dynamic>);
}
