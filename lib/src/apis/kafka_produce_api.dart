import 'dart:typed_data';

import 'package:dart_kafka/src/models/partition.dart';
import 'package:dart_kafka/src/models/topic.dart';
import 'package:dart_kafka/src/protocol/apis.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/protocol/errors.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaProduceApi {
  final int apiKey = PRODUCE; // ProduceRequest API key
  final Utils utils = Utils();

  /// Serialize the ProduceRequest
  Uint8List serialize(
      {required int correlationId,
      String? transactionalId,
      required int acks,
      required int timeoutMs,
      required List<Topic> topics,
      int apiVersion = 11,
      String? clientId,
      required int producerId,
      required int attributes,
      required int lastOffsetDelta,
      required int producerEpoch,
      required int baseSequence,
      required int batchOffset,
      required int partitionLeaderEpoch}) {
    BytesBuilder byteBuffer = BytesBuilder();
    Encoder encoder = Encoder();

    if (apiVersion >= 9) {
      byteBuffer.add(utils.compactNullableString(transactionalId));
    } else {
      byteBuffer.add(utils.nullableString(transactionalId));
    }

    byteBuffer.add(utils.int16(acks));
    byteBuffer.add(utils.int32(timeoutMs));

    if (apiVersion >= 9) {
      byteBuffer.add(utils.compactArrayLength(topics.length));
    } else {
      byteBuffer.add(utils.int32(topics.length));
    }

    for (final topic in topics) {
      if (apiVersion >= 9) {
        byteBuffer.add(utils.compactString(topic.topicName));
        byteBuffer.add(utils.compactArrayLength(topic.partitions?.length ?? 0));
      } else {
        byteBuffer.add(utils.string(topic.topicName));
        byteBuffer.add(utils.int32(topic.partitions?.length ?? 0));
      }

      for (Partition partition in topic.partitions ?? []) {
        if (partition.batch == null) {
          throw Exception(
              "The partition ${partition.partitionId} has a null batch.");
        }

        if (partition.batch!.records == null) {
          throw Exception(
              "The batch of the partition ${partition.partitionId} has null records");
        }

        byteBuffer.add(utils.int32(partition.partitionId));
        final Uint8List recordBatch = encoder.writeRecordBatch(
            records: partition.batch!.records!,
            producerId: producerId,
            attributes: attributes,
            lastOffsetDelta: lastOffsetDelta,
            producerEpoch: producerEpoch,
            baseSequence: baseSequence,
            batchOffset: batchOffset,
            partitionLeaderEpoch: partitionLeaderEpoch);

        byteBuffer = utils.writeUnsignedVarint(recordBatch.length + 1,
            byteBuffer); // adding the size of the RecordBatch
        byteBuffer.add(recordBatch);
      }
    }

    if (apiVersion >= 9) {
      // Adding the _tagged_fields
      byteBuffer.addByte(0);
      byteBuffer.addByte(0);
      byteBuffer.addByte(0);
    }

    final message = byteBuffer.toBytes();
    byteBuffer.clear();

    return Uint8List.fromList([
      ...encoder.writeMessageHeader(
          apiKey: apiKey,
          apiVersion: apiVersion,
          clientId: clientId,
          correlationId: correlationId,
          messageLength: message.length,
          version: apiVersion > 8 ? 2 : 1),
      ...message
    ]);
  }

  /// Deserialize the ProduceResponse
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 1; // ignore the tagged_buffer

    // Read responses array
    final result = utils.readCompactArrayLength(buffer, offset);
    final responsesLength = result.value;
    offset += result.bytesRead;

    final responses = <Map<String, dynamic>>[];
    for (int i = 0; i < responsesLength; i++) {
      final topic = utils.readCompactString(buffer, offset);
      final String topicName = topic.value;
      offset += topic.bytesRead;

      // Read partition_responses array
      final partitions = utils.readCompactArrayLength(buffer, offset);
      final partitionResponsesLength = partitions.value;
      offset += partitions.bytesRead;

      final partitionResponses = <Map<String, dynamic>>[];
      for (int j = 0; j < partitionResponsesLength; j++) {
        final partition = buffer.getInt32(offset);
        offset += 4;
        final errorCode = buffer.getInt16(offset);
        offset += 2;
        final baseOffset = buffer.getInt64(offset);
        offset += 8;
        final logAppendTime = buffer.getInt64(offset);
        offset += 8;
        final logStartOffset = buffer.getInt64(offset);
        offset += 8;

        final errors = utils.readCompactArrayLength(buffer, offset);
        offset += errors.bytesRead;

        final errorResponses = <Map<int, String?>>[];
        for (int k = 0; k < errors.value; k++) {
          final int batchIndex = buffer.getInt32(offset);
          final batchErrorIndex =
              utils.readCompactNullableString(buffer, offset);
          offset += batchErrorIndex.bytesRead;
          final int taggedFields = buffer.getInt8(offset);

          errorResponses.add({batchIndex: batchErrorIndex.value});
        }

        final errorMessage = utils.readCompactNullableString(buffer, offset);
        offset += errorMessage.bytesRead;
        final int taggedFields1 = buffer.getInt8(offset);
        offset += 1;
        final int taggedFields2 = buffer.getInt8(offset);
        offset += 1;
        final int throttleTimeMs = buffer.getInt32(offset);
        offset += 4;
        final int taggedFields3 = buffer.getInt8(offset);
        offset += 1;

        partitionResponses.add({
          'partition': partition,
          'error': {'code': errorCode, 'message': ERROR_MAP[errorCode]},
          'error_msg': errorMessage.value,
          'base_offset': baseOffset,
          'log_append_time': logAppendTime,
          'log_start_offset': logStartOffset,
        });
      }

      responses.add({
        'topic': topic.value,
        'partition_responses': partitionResponses,
      });
    }

    var throttleTimeMs;

    if (utils.canRead(currentOffset: offset, amountOfBytes: 4, data: data)) {
      throttleTimeMs = buffer.getInt32(offset);
      offset += 4;
    }

    // Read TAG_BUFFER (empty for now)
    utils.readTagBuffer(buffer, offset);

    return {
      'responses': responses,
      'throttle_time_ms': throttleTimeMs,
    };
  }
}
