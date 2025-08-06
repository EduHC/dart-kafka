// ignore_for_file: public_member_api_docs, sort_constructors_first
import 'dart:convert';

import 'components/aborted_transactions.dart';
import 'components/active_producer.dart';
import 'components/record_batch.dart';
import 'components/record_error.dart';

class Partition {
  final int id;
  final int? fetchOffset;
  final int? logStartOffset;
  final int? maxBytes;

  // fields used by the FetchResponse
  final int? errorCode;
  final int? highWatermark;
  final int? lastStableOffset;
  final List<AbortedTransaction>? abortedTransactions;
  final RecordBatch? batch;

  // fields used by DescribeProducersResponse
  final String? errorMessage;
  final List<ActiveProducer>? activeProducers;

  // fields used by ProduceResponse
  final int? baseOffset;
  final int? logAppendTimeMs;
  final List<RecordError>? recordErrors;
  final int? throttleTimeMs;

  // fields used by listOffsetsResponse
  final int? timestamp;
  final int? offset;
  final int? leaderEpoch;

  Partition({
    required this.id,
    this.fetchOffset,
    this.logStartOffset,
    this.maxBytes,
    this.abortedTransactions,
    this.errorCode,
    this.lastStableOffset,
    this.highWatermark,
    this.batch,
    this.errorMessage,
    this.activeProducers,
    this.baseOffset,
    this.logAppendTimeMs,
    this.recordErrors,
    this.throttleTimeMs,
    this.timestamp,
    this.offset,
    this.leaderEpoch,
  });

  @override
  String toString() =>
      'Partition -> id: $id, fetchOffset: $fetchOffset, logStartOffset: $logStartOffset, maxBytes: $maxBytes, '
      'errorCode: $errorCode, error: $errorMessage, highWatermark: $highWatermark, lastStableOffset: $lastStableOffset, '
      'abortedTransactions: $abortedTransactions, batch: $batch, activeProducers: $activeProducers, baseOffset: $baseOffset, '
      'logAppendTimeMs: $logAppendTimeMs, recordErrors: $recordErrors, throttleTimeMs: $throttleTimeMs, '
      'timestamp: $timestamp, offset: $offset, leaderEpoch: $leaderEpoch';

  Map<String, dynamic> toMap() => <String, dynamic>{
        'id': id,
        'fetchOffset': fetchOffset,
        'logStartOffset': logStartOffset,
        'maxBytes': maxBytes,
        'errorCode': errorCode,
        'highWatermark': highWatermark,
        'lastStableOffset': lastStableOffset,
        'abortedTransactions':
            abortedTransactions?.map((x) => x.toMap()).toList(),
        'batch': batch?.toMap(),
        'errorMessage': errorMessage,
        'activeProducers': activeProducers?.map((x) => x.toMap()).toList(),
        'baseOffset': baseOffset,
        'logAppendTimeMs': logAppendTimeMs,
        'recordErrors': recordErrors?.map((x) => x.toMap()).toList(),
        'throttleTimeMs': throttleTimeMs,
        'timestamp': timestamp,
        'offset': offset,
        'leaderEpoch': leaderEpoch,
      };

  factory Partition.fromMap(Map<String, dynamic> map) => Partition(
        id: map['id'] as int,
        fetchOffset:
            map['fetchOffset'] != null ? map['fetchOffset'] as int : null,
        logStartOffset:
            map['logStartOffset'] != null ? map['logStartOffset'] as int : null,
        maxBytes: map['maxBytes'] != null ? map['maxBytes'] as int : null,
        errorCode: map['errorCode'] != null ? map['errorCode'] as int : null,
        highWatermark:
            map['highWatermark'] != null ? map['highWatermark'] as int : null,
        lastStableOffset: map['lastStableOffset'] != null
            ? map['lastStableOffset'] as int
            : null,
        abortedTransactions: map['abortedTransactions'] != null
            ? List<AbortedTransaction>.from(
                (map['abortedTransactions'] as List<dynamic>)
                    .map<AbortedTransaction?>(
                  (x) => AbortedTransaction.fromMap(x as Map<String, dynamic>),
                ),
              )
            : null,
        batch: map['batch'] != null
            ? RecordBatch.fromMap(map['batch'] as Map<String, dynamic>)
            : null,
        errorMessage:
            map['errorMessage'] != null ? map['errorMessage'] as String : null,
        activeProducers: map['activeProducers'] != null
            ? List<ActiveProducer>.from(
                (map['activeProducers'] as List<dynamic>).map<ActiveProducer?>(
                  (x) => ActiveProducer.fromMap(x as Map<String, dynamic>),
                ),
              )
            : null,
        baseOffset: map['baseOffset'] != null ? map['baseOffset'] as int : null,
        logAppendTimeMs: map['logAppendTimeMs'] != null
            ? map['logAppendTimeMs'] as int
            : null,
        recordErrors: map['recordErrors'] != null
            ? List<RecordError>.from(
                (map['recordErrors'] as List<dynamic>).map<RecordError?>(
                  (x) => RecordError.fromMap(x as Map<String, dynamic>),
                ),
              )
            : null,
        throttleTimeMs:
            map['throttleTimeMs'] != null ? map['throttleTimeMs'] as int : null,
        timestamp: map['timestamp'] != null ? map['timestamp'] as int : null,
        offset: map['offset'] != null ? map['offset'] as int : null,
        leaderEpoch:
            map['leaderEpoch'] != null ? map['leaderEpoch'] as int : null,
      );

  String toJson() => json.encode(toMap());

  factory Partition.fromJson(String source) =>
      Partition.fromMap(json.decode(source) as Map<String, dynamic>);
}
