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
  String toString() {
    return "ActiveProducer -> id: $id, epoch: $epoch, lastSequence: $lastSequence, "
        "lastTimestamp: $lastTimestamp, coordinatorEpoch: $coordinatorEpoch, "
        "currentTxnStartOffset: $currentTxnStartOffset";
  }
}
