import 'dart:typed_data';

import 'package:dart_kafka/src/models/components/aborted_transactions.dart';
import 'package:dart_kafka/src/models/components/record_batch.dart';
import 'package:dart_kafka/src/models/components/record.dart';
import 'package:dart_kafka/src/models/components/record_header.dart';
import 'package:dart_kafka/src/models/partition.dart';
import 'package:dart_kafka/src/models/responses/fetch_response.dart';
import 'package:dart_kafka/src/models/topic.dart';
import 'package:dart_kafka/src/protocol/utils.dart';
import 'package:dart_kafka/src/protocol/apis.dart';

class KafkaFetchApi {
  final int apiKey;
  Utils utils = Utils();

  KafkaFetchApi() : apiKey = FETCH;

  /// Method to serialize to build and serialize the FetchRequest to Byte Array
  Uint8List serialize({
    required int correlationId,
    required int apiVersion,
    required String? clientId,
    int? replicaId,
    int? maxWaitMs,
    int? minBytes,
    int? maxBytes,
    required int isolationLevel,
    required List<Topic> topics,
  }) {
    final byteBuffer = BytesBuilder();

    byteBuffer.add(utils.int16(apiKey));
    byteBuffer.add(utils.int16(apiVersion));
    byteBuffer.add(utils.int32(correlationId));

    if (clientId != null) {
      final clientIdBytes = clientId.codeUnits;
      byteBuffer.add(utils.int16(clientIdBytes.length));
      byteBuffer.add(clientIdBytes);
    } else {
      byteBuffer.add(utils.int16(-1));
    }

    byteBuffer.add(utils.int32(replicaId ?? -1));
    byteBuffer.add(utils.int32(maxWaitMs ?? 30000));
    byteBuffer.add(utils.int32(minBytes ?? 1));
    byteBuffer.add(utils.int32(maxBytes ?? 10000));

    // Add isolation level (0 = read_uncommitted, 1 = read_committed)
    byteBuffer.add(utils.int8(isolationLevel));

    // Add session ID and epoch (only for versions >= 7)
    if (apiVersion >= 7) {
      byteBuffer.add(utils.int32(0)); // Session ID (default to 0)
      byteBuffer.add(utils.int32(0)); // Session epoch (default to 0)
    }

    // Add topics to fetch
    byteBuffer.add(utils.int32(topics.length));
    for (final topic in topics) {
      final topicNameBytes = topic.topicName.codeUnits;
      byteBuffer.add(utils.int16(topicNameBytes.length));
      byteBuffer.add(topicNameBytes);

      byteBuffer.add(utils.int32(topic.partitions.length));
      for (final partition in topic.partitions) {
        byteBuffer.add(utils.int32(partition.partitionId));
        byteBuffer.add(utils.int64(partition.fetchOffset ?? 0));
        byteBuffer.add(utils.int64(partition
            .logStartOffset)); // Log start offset (only for versions >= 5)
        byteBuffer.add(utils.int32(partition.maxBytes ?? 45));
      }
    }

    // Add forgotten topics (only for versions >= 7)
    if (apiVersion >= 7) {
      byteBuffer
          .add(utils.int32(0)); // Number of forgotten topics (default to 0)
    }

    // Add rack ID (only for versions >= 11)
    if (apiVersion >= 11) {
      byteBuffer.add(utils.int16(-1)); // Rack ID (default to null)
    }

    Uint8List message = byteBuffer.toBytes();
    return Uint8List.fromList([...utils.int32(message.length), ...message]);
  }

  /// Method to deserialize the FetchResponse from a Byte Array
  dynamic deserialize(Uint8List data, int apiVersion) {
    if (data.length < 14) {
      print("Invalid byte array: Insufficient length for FetchResponse");
      return null;
    }

    final buffer = ByteData.sublistView(data);
    int offset = 0;

    // Read the response fields
    final int throttleTimeMs = buffer.getInt32(offset, Endian.big);
    offset += 4;

    final int errorCode = buffer.getInt16(offset, Endian.big);
    offset += 2;

    final int sessionId =
        apiVersion >= 7 ? buffer.getInt32(offset, Endian.big) : 0;
    offset += apiVersion >= 7 ? 4 : 0;

    final List<Topic> topics = [];
    final int topicsLength = buffer.getInt32(offset, Endian.big);
    offset += 4;

    for (int i = 0; i < topicsLength; i++) {
      final int topicNameLength = buffer.getInt16(offset, Endian.big);
      offset += 2;

      final String topicName = String.fromCharCodes(
          buffer.buffer.asUint8List(offset, topicNameLength));
      offset += topicNameLength;

      final List<Partition> partitions = [];
      final int partitionsLength = buffer.getInt32(offset, Endian.big);
      offset += 4;

      for (int j = 0; j < partitionsLength; j++) {
        final int partitionId = buffer.getInt32(offset, Endian.big);
        offset += 4;

        final int partitionErrorCode = buffer.getInt16(offset, Endian.big);
        offset += 2;

        final int highWatermark = buffer.getInt64(offset, Endian.big);
        offset += 8;

        final int lastStableOffset =
            apiVersion >= 4 ? buffer.getInt64(offset, Endian.big) : -1;
        offset += apiVersion >= 4 ? 8 : 0;

        final int logStartOffset =
            apiVersion >= 5 ? buffer.getInt64(offset, Endian.big) : -1;
        offset += apiVersion >= 5 ? 8 : 0;

        final int abortedTransactionsLength =
            apiVersion >= 4 ? buffer.getInt32(offset, Endian.big) : 0;
        offset += apiVersion >= 4 ? 4 : 0;

        final List<AbortedTransaction> abortedTransactions = [];
        for (int k = 0; k < abortedTransactionsLength; k++) {
          final int producerId = buffer.getInt64(offset, Endian.big);
          offset += 8;

          final int firstOffset = buffer.getInt64(offset, Endian.big);
          offset += 8;

          abortedTransactions.add(AbortedTransaction(
            producerId: producerId,
            firstOffset: firstOffset,
          ));
        }

        final int recordsLength = buffer.getInt32(offset, Endian.big);
        offset += 4;

        final RecordBatch? batch = _decodeRecordBatch(
            buffer.buffer.asUint8List(offset, recordsLength), apiVersion);
        offset += recordsLength;

        partitions.add(Partition(
          partitionId: partitionId,
          errorCode: partitionErrorCode,
          highWatermark: highWatermark,
          lastStableOffset: lastStableOffset,
          logStartOffset: logStartOffset,
          abortedTransactions: abortedTransactions,
          records: batch,
        ));
      }

      topics.add(Topic(
        topicName: topicName,
        partitions: partitions,
      ));
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

    final int baseOffset = buffer.getInt64(offset, Endian.big);
    offset += 8;

    final int batchLength = buffer.getInt32(offset, Endian.big);
    offset += 4;

    final int partitionLeaderEpoch = buffer.getInt32(offset, Endian.big);
    offset += 4;

    final int magic = buffer.getInt8(offset);
    offset += 1;

    final int crc = buffer.getUint32(offset, Endian.big);
    offset += 4;

    final int attributes = buffer.getInt16(offset, Endian.big);
    offset += 2;

    final int lastOffsetDelta = buffer.getInt32(offset, Endian.big);
    offset += 4;

    final int baseTimestamp = buffer.getInt64(offset, Endian.big);
    offset += 8;

    final int maxTimestamp = buffer.getInt64(offset, Endian.big);
    offset += 8;

    final int producerId = buffer.getInt64(offset, Endian.big);
    offset += 8;

    final int producerEpoch = buffer.getInt16(offset, Endian.big);
    offset += 2;

    final int baseSequence = buffer.getInt32(offset, Endian.big);
    offset += 4;

    final int recordsLength = buffer.getInt32(offset, Endian.big);
    offset += 4;

    final List<Record> records = [];

    for (int i = 0; i < recordsLength; i++) {
      var result = utils.readVarint(data.toList(), offset, signed: false);
      final int recordLength = result.value;
      offset += result.bytesRead;

      final int attributes = buffer.getInt8(offset);
      offset += 1;

      result = utils.readVarint(
        data.toList(),
        offset,
      );
      final int timestampDelta = result.value;
      offset += result.bytesRead;

      result = utils.readVarint(
        data.toList(),
        offset,
      );
      final int offsetDelta = result.value;
      offset += result.bytesRead;

      result = utils.readVarint(
        data.toList(),
        offset,
      );
      final int keyLength = result.value;
      offset += result.bytesRead;

      final String? key = keyLength == -1
          ? null
          : String.fromCharCodes(buffer.buffer.asUint8List(offset, keyLength));
      offset += keyLength == -1 ? 0 : keyLength;

      result = utils.readVarint(
        data.toList(),
        offset,
      );
      final int valueLength = result.value;
      offset += result.bytesRead;

      final String? value = valueLength == -1
          ? null
          : String.fromCharCodes(
              buffer.buffer.asUint8List(offset, valueLength));
      offset += valueLength == -1 ? 0 : valueLength;

      bool hasHeaders = utils.canRead(
          currentOffset: offset, amountOfBytes: 4, data: data.toList());

      if (hasHeaders) {
        result = utils.readVarint(
          data.toList(),
          offset,
        );
      }

      final int headersLength = hasHeaders ? result.value : 0;
      offset += hasHeaders ? result.bytesRead : 0;

      final List<RecordHeader> headers = [];
      for (int j = 0; j < headersLength; j++) {
        result = utils.readVarint(
          data.toList(),
          offset,
        );
        final int headerKeyLength = result.value;
        offset += result.bytesRead;

        final String headerKey = String.fromCharCodes(
            buffer.buffer.asUint8List(offset, headerKeyLength));
        offset += headerKeyLength;

        result = utils.readVarint(data.toList(), offset);
        final int headerValueLength = result.value;
        offset += result.bytesRead;

        final String headerValue = String.fromCharCodes(
            buffer.buffer.asUint8List(offset, headerValueLength));
        offset += headerValueLength;

        headers.add(RecordHeader(
          headerKey: headerKey,
          headerKeyLength: headerKeyLength,
          headerValue: headerValue,
          headerValueLength: headerValueLength,
        ));
      }

      records.add(Record(
        length: recordLength,
        attributes: attributes,
        timestampDelta: timestampDelta,
        offsetDelta: offsetDelta,
        key: key,
        value: value,
        headers: headers,
      ));
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
