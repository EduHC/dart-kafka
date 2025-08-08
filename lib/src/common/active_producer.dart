import 'dart:convert';

class ActiveProducer {
  ActiveProducer({
    required this.id,
    required this.epoch,
    required this.lastSequence,
    required this.lastTimestamp,
    required this.coordinatorEpoch,
    required this.currentTxnStartOffset,
  });

  factory ActiveProducer.fromMap(Map<String, dynamic> map) => ActiveProducer(
        id: map['id'] as int,
        epoch: map['epoch'] as int,
        lastSequence: map['lastSequence'] as int,
        lastTimestamp: map['lastTimestamp'] as int,
        coordinatorEpoch: map['coordinatorEpoch'] as int,
        currentTxnStartOffset: map['currentTxnStartOffset'] as int,
      );

  factory ActiveProducer.fromJson(String source) =>
      ActiveProducer.fromMap(json.decode(source) as Map<String, dynamic>);
  final int id;
  final int epoch;
  final int lastSequence;
  final int lastTimestamp;
  final int coordinatorEpoch;
  final int currentTxnStartOffset;

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

  String toJson() => json.encode(toMap());
}
