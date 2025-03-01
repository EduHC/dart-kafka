import 'dart:typed_data';

import 'package:dart_kafka/src/models/components/aborted_transactions.dart';

class Partition {
  final int partitionId;
  final int? fetchOffset;
  final int logStartOffset;
  final int? maxBytes;

  // fields used by the FetchResponse
  final int? errorCode;
  final int? highWatermark;
  final int? lastStableOffset;
  final List<AbortedTransaction>? abortedTransactions;
  final Uint8List? records;

  Partition(
      {required this.partitionId,
      this.fetchOffset,
      required this.logStartOffset,
      this.maxBytes,
      this.abortedTransactions,
      this.errorCode,
      this.lastStableOffset,
      this.highWatermark,
      this.records});

  @override
  String toString() {
    return "Partition -> id: $partitionId, fetchOffset: $fetchOffset, logStartOffset: $logStartOffset, maxBytes: $maxBytes, "
        "errorCode: $errorCode, highWatermark: $highWatermark, lastStableOffset: $lastStableOffset, "
        "abortedTransactions: $abortedTransactions, records: $records";
  }
}
