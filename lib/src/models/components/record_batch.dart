import 'package:dart_kafka/src/models/components/record.dart';

class RecordBatch {
  final int? baseOffset;
  final int? batchLength;
  final int? partitionLeaderEpoch;
  final int? magic;
  final int? crc;
  final int? attributes;
  final int? lastOffsetDelta;
  final int? baseTimestamp;
  final int? maxTimestamp;
  final int? producerId;
  final int? producerEpoch;
  final int? baseSequence;
  final List<Record>? records;

  RecordBatch(
      {this.baseOffset,
      this.batchLength,
      this.partitionLeaderEpoch,
      this.magic,
      this.crc,
      this.attributes,
      this.lastOffsetDelta,
      this.baseTimestamp,
      this.maxTimestamp,
      this.producerId,
      this.producerEpoch,
      this.baseSequence,
      this.records});

  @override
  String toString() {
    return "RecordBatch -> baseOffset: $baseOffset, batchLength: $batchLength, "
        "partitionLeaderEpoch: $partitionLeaderEpoch, magic: $magic, crc: $crc"
        "attributes: $attributes, lastOffsetDelta: $lastOffsetDelta, "
        "baseTimestamp: $baseTimestamp, maxTimestamp: $maxTimestamp, "
        "producerId: $producerId, producerEpoch: $producerEpoch, "
        "baseSequence: $baseSequence, records: $records";
  }
}
