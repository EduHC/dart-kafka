import 'package:dart_kafka/src/models/components/aborted_transactions.dart';
import 'package:dart_kafka/src/models/components/active_producer.dart';
import 'package:dart_kafka/src/models/components/record_batch.dart';
import 'package:dart_kafka/src/models/components/record_error.dart';

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
  String toString() {
    return "Partition -> id: $id, fetchOffset: $fetchOffset, logStartOffset: $logStartOffset, maxBytes: $maxBytes, "
        "errorCode: $errorCode, error: $errorMessage, highWatermark: $highWatermark, lastStableOffset: $lastStableOffset, "
        "abortedTransactions: $abortedTransactions, batch: $batch, activeProducers: $activeProducers, baseOffset: $baseOffset, "
        "logAppendTimeMs: $logAppendTimeMs, recordErrors: $recordErrors, throttleTimeMs: $throttleTimeMs, "
        "timestamp: $timestamp, offset: $offset, leaderEpoch: $leaderEpoch";
  }
}
