import 'dart:typed_data';

import '../../common/aborted_transactions.dart';
import '../../common/partition.dart';
import '../../common/record.dart';
import '../../common/record_batch.dart';
import '../../common/record_header.dart';
import '../../common/topic.dart';
import '../../protocol/decoder.dart';
import '../../protocol/definition/api.dart';
import '../../protocol/encoder.dart';
import '../../protocol/utils.dart';
import 'fetch_response.dart';

class KafkaFetchApi {
  final int apiKey = FETCH;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Method to serialize to build and serialize the FetchRequest to Byte Array
  Uint8List serialize({
    required int correlationId,
    required int apiVersion,
    required String? clientId,
    required int replicaId,
    required int maxWaitMs,
    required int minBytes,
    required int maxBytes,
    required int isolationLevel,
    required List<Topic> topics,
  }) {
    final byteBuffer = BytesBuilder()
      ..add(encoder.int16(apiKey))
      ..add(encoder.int16(apiVersion))
      ..add(encoder.int32(correlationId));

    if (clientId != null) {
      final clientIdBytes = clientId.codeUnits;
      byteBuffer
        ..add(encoder.int16(clientIdBytes.length))
        ..add(clientIdBytes);
    } else {
      byteBuffer.add(encoder.int16(-1));
    }

    byteBuffer
      ..add(encoder.int32(replicaId))
      ..add(encoder.int32(maxWaitMs))
      ..add(encoder.int32(minBytes))
      ..add(encoder.int32(maxBytes))

      // Add isolation level (0 = read_uncommitted, 1 = read_committed)
      ..add(encoder.int8(isolationLevel));

    // Add session ID and epoch (only for versions >= 7)
    if (apiVersion >= 7) {
      byteBuffer
        ..add(encoder.int32(0)) // Session ID (default to 0)
        ..add(encoder.int32(0)); // Session epoch (default to 0)
    }

    // Add topics to fetch
    byteBuffer.add(encoder.int32(topics.length));
    for (final topic in topics) {
      final topicNameBytes = topic.topicName.codeUnits;
      byteBuffer
        ..add(encoder.int16(topicNameBytes.length))
        ..add(topicNameBytes)
        ..add(encoder.int32(topic.partitions!.length));
      for (final partition in topic.partitions!) {
        byteBuffer
          ..add(encoder.int32(partition.id))
          ..add(encoder.int64(partition.fetchOffset ?? 0))
          ..add(
            encoder.int64(
              partition.logStartOffset ?? 0,
            ),
          ) // Log start offset (only for versions >= 5)
          ..add(encoder.int32(partition.maxBytes ?? 45));
      }
    }

    // Add forgotten topics (only for versions >= 7)
    if (apiVersion >= 7) {
      byteBuffer
          .add(encoder.int32(0)); // Number of forgotten topics (default to 0)
    }

    // Add rack ID (only for versions >= 11)
    if (apiVersion >= 11) {
      byteBuffer.add(encoder.int16(-1)); // Rack ID (default to null)
    }

    final Uint8List message = byteBuffer.toBytes();
    return Uint8List.fromList([...encoder.int32(message.length), ...message]);
  }

  /// Method to deserialize the FetchResponse from a Byte Array
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    // Read the response fields
    final int throttleTimeMs = buffer.getInt32(offset);
    offset += 4;

    final int errorCode = buffer.getInt16(offset);
    offset += 2;

    final int sessionId = apiVersion >= 7 ? buffer.getInt32(offset) : 0;
    offset += apiVersion >= 7 ? 4 : 0;

    final List<Topic> topics = [];
    final int topicsLength = buffer.getInt32(offset);
    offset += 4;

    for (int i = 0; i < topicsLength; i++) {
      final int topicNameLength = buffer.getInt16(offset);
      offset += 2;

      final String topicName = String.fromCharCodes(
        buffer.buffer.asUint8List(offset, topicNameLength),
      );
      offset += topicNameLength;

      final List<Partition> partitions = [];
      final int partitionsLength = buffer.getInt32(offset);
      offset += 4;

      for (int j = 0; j < partitionsLength; j++) {
        final int partitionId = buffer.getInt32(offset);
        offset += 4;

        final int partitionErrorCode = buffer.getInt16(offset);
        offset += 2;

        final int highWatermark = buffer.getInt64(offset);
        offset += 8;

        final int lastStableOffset =
            apiVersion >= 4 ? buffer.getInt64(offset) : -1;
        offset += apiVersion >= 4 ? 8 : 0;

        final int logStartOffset =
            apiVersion >= 5 ? buffer.getInt64(offset) : -1;
        offset += apiVersion >= 5 ? 8 : 0;

        final int abortedTransactionsLength =
            apiVersion >= 4 ? buffer.getInt32(offset) : 0;
        offset += apiVersion >= 4 ? 4 : 0;

        final List<AbortedTransaction> abortedTransactions = [];
        for (int k = 0; k < abortedTransactionsLength; k++) {
          final int producerId = buffer.getInt64(offset);
          offset += 8;

          final int firstOffset = buffer.getInt64(offset);
          offset += 8;

          abortedTransactions.add(
            AbortedTransaction(
              producerId: producerId,
              firstOffset: firstOffset,
            ),
          );
        }

        final int recordsLength = buffer.getInt32(offset);
        offset += 4;

        final RecordBatch? batch = _decodeRecordBatch(
          buffer.buffer.asUint8List(offset, recordsLength),
          apiVersion,
        );
        offset += recordsLength;

        partitions.add(
          Partition(
            id: partitionId,
            errorCode: partitionErrorCode,
            highWatermark: highWatermark,
            lastStableOffset: lastStableOffset,
            logStartOffset: logStartOffset,
            abortedTransactions: abortedTransactions,
            batch: batch,
          ),
        );
      }

      topics.add(
        Topic(
          topicName: topicName,
          partitions: partitions,
        ),
      );
    }

    return FetchResponse(
      throttleTimeMs: throttleTimeMs,
      errorCode: errorCode,
      sessionId: sessionId,
      topics: topics,
    );
  }

  RecordBatch? _decodeRecordBatch(Uint8List data, int apiVersion) {
    if (data.isEmpty) return null;

    final buffer = ByteData.sublistView(Uint8List.fromList(data.toList()));
    int offset = 0;

    final int baseOffset = buffer.getInt64(offset);
    offset += 8;

    final int batchLength = buffer.getInt32(offset);
    offset += 4;

    final int partitionLeaderEpoch = buffer.getInt32(offset);
    offset += 4;

    final int magic = buffer.getInt8(offset);
    offset += 1;

    final int crc = buffer.getUint32(offset);
    offset += 4;

    final int attributes = buffer.getInt16(offset);
    offset += 2;

    final int lastOffsetDelta = buffer.getInt32(offset);
    offset += 4;

    final int baseTimestamp = buffer.getInt64(offset);
    offset += 8;

    final int maxTimestamp = buffer.getInt64(offset);
    offset += 8;

    final int producerId = buffer.getInt64(offset);
    offset += 8;

    final int producerEpoch = buffer.getInt16(offset);
    offset += 2;

    final int baseSequence = buffer.getInt32(offset);
    offset += 4;

    final int recordsLength = buffer.getInt32(offset);
    offset += 4;

    final List<Record> records = [];

    for (int i = 0; i < recordsLength; i++) {
      var result = decoder.readVarint(data.toList(), offset, signed: false);
      final int recordLength = result.value;
      offset += result.bytesRead;

      final int attributes = buffer.getInt8(offset);
      offset += 1;

      result = decoder.readVarint(
        data.toList(),
        offset,
      );
      final int timestampDelta = result.value;
      offset += result.bytesRead;

      result = decoder.readVarint(
        data.toList(),
        offset,
      );
      final int offsetDelta = result.value;
      offset += result.bytesRead;

      result = decoder.readVarint(
        data.toList(),
        offset,
      );
      final int keyLength = result.value;
      offset += result.bytesRead;

      final String? key = keyLength == -1
          ? null
          : String.fromCharCodes(buffer.buffer.asUint8List(offset, keyLength));
      offset += keyLength == -1 ? 0 : keyLength;

      result = decoder.readVarint(
        data.toList(),
        offset,
      );
      final int valueLength = result.value;
      offset += result.bytesRead;

      final String? value = valueLength == -1
          ? null
          : String.fromCharCodes(
              buffer.buffer.asUint8List(offset, valueLength),
            );
      offset += valueLength == -1 ? 0 : valueLength;

      final bool hasHeaders = utils.canRead(
        currentOffset: offset,
        amountOfBytes: 4,
        data: data.toList(),
      );

      if (hasHeaders) {
        result = decoder.readVarint(
          data.toList(),
          offset,
        );
      }

      final int headersLength = hasHeaders ? result.value : 0;
      offset += hasHeaders ? result.bytesRead : 0;

      final List<RecordHeader> headers = [];
      for (int j = 0; j < headersLength; j++) {
        result = decoder.readVarint(
          data.toList(),
          offset,
        );
        final int headerKeyLength = result.value;
        offset += result.bytesRead;

        final String headerKey = String.fromCharCodes(
          buffer.buffer.asUint8List(offset, headerKeyLength),
        );
        offset += headerKeyLength;

        result = decoder.readVarint(data.toList(), offset);
        final int headerValueLength = result.value;
        offset += result.bytesRead;

        final String headerValue = String.fromCharCodes(
          buffer.buffer.asUint8List(offset, headerValueLength),
        );
        offset += headerValueLength;

        headers.add(
          RecordHeader(
            key: headerKey,
            value: headerValue,
          ),
        );
      }

      records.add(
        Record(
          length: recordLength,
          attributes: attributes,
          timestampDelta: timestampDelta,
          offsetDelta: offsetDelta,
          key: key,
          value: value,
          headers: headers,
          timestamp: DateTime.now().millisecondsSinceEpoch,
        ),
      );
    }

    return RecordBatch(
      baseOffset: baseOffset,
      batchLength: batchLength,
      partitionLeaderEpoch: partitionLeaderEpoch,
      magic: magic,
      crc: crc,
      attributes: attributes,
      lastOffsetDelta: lastOffsetDelta,
      baseTimestamp: baseTimestamp,
      maxTimestamp: maxTimestamp,
      producerId: producerId,
      producerEpoch: producerEpoch,
      baseSequence: baseSequence,
      records: records,
    );
  }
}
