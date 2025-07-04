// ignore_for_file: public_member_api_docs, sort_constructors_first
import 'dart:convert';

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

  RecordBatch({
    this.baseOffset,
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
    this.records,
  });

  @override
  String toString() {
    return "RecordBatch -> baseOffset: $baseOffset, batchLength: $batchLength, "
        "partitionLeaderEpoch: $partitionLeaderEpoch, magic: $magic, crc: $crc"
        "attributes: $attributes, lastOffsetDelta: $lastOffsetDelta, "
        "baseTimestamp: $baseTimestamp, maxTimestamp: $maxTimestamp, "
        "producerId: $producerId, producerEpoch: $producerEpoch, "
        "baseSequence: $baseSequence, records: $records";
  }

  Map<String, dynamic> toMap() {
    return <String, dynamic>{
      'baseOffset': baseOffset,
      'batchLength': batchLength,
      'partitionLeaderEpoch': partitionLeaderEpoch,
      'magic': magic,
      'crc': crc,
      'attributes': attributes,
      'lastOffsetDelta': lastOffsetDelta,
      'baseTimestamp': baseTimestamp,
      'maxTimestamp': maxTimestamp,
      'producerId': producerId,
      'producerEpoch': producerEpoch,
      'baseSequence': baseSequence,
      'records': records?.map((x) => x.toMap()).toList(),
    };
  }

  factory RecordBatch.fromMap(Map<String, dynamic> map) {
    return RecordBatch(
      baseOffset: map['baseOffset'] != null ? map['baseOffset'] as int : null,
      batchLength:
          map['batchLength'] != null ? map['batchLength'] as int : null,
      partitionLeaderEpoch: map['partitionLeaderEpoch'] != null
          ? map['partitionLeaderEpoch'] as int
          : null,
      magic: map['magic'] != null ? map['magic'] as int : null,
      crc: map['crc'] != null ? map['crc'] as int : null,
      attributes: map['attributes'] != null ? map['attributes'] as int : null,
      lastOffsetDelta:
          map['lastOffsetDelta'] != null ? map['lastOffsetDelta'] as int : null,
      baseTimestamp:
          map['baseTimestamp'] != null ? map['baseTimestamp'] as int : null,
      maxTimestamp:
          map['maxTimestamp'] != null ? map['maxTimestamp'] as int : null,
      producerId: map['producerId'] != null ? map['producerId'] as int : null,
      producerEpoch:
          map['producerEpoch'] != null ? map['producerEpoch'] as int : null,
      baseSequence:
          map['baseSequence'] != null ? map['baseSequence'] as int : null,
      records: map['records'] != null
          ? List<Record>.from(
              (map['records'] as List<dynamic>).map<Record?>(
                (x) => Record.fromMap(x as Map<String, dynamic>),
              ),
            )
          : null,
    );
  }

  String toJson() => json.encode(toMap());

  factory RecordBatch.fromJson(String source) =>
      RecordBatch.fromMap(json.decode(source) as Map<String, dynamic>);
}
