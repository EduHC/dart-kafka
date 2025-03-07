import 'dart:convert';
import 'dart:typed_data';

import 'package:dart_kafka/src/models/partition.dart';
import 'package:dart_kafka/src/models/topic.dart';
import 'package:dart_kafka/src/protocol/apis.dart';
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
      int? apiVersion,
      String? clientId}) {
    final byteBuffer = BytesBuilder();

    byteBuffer.add(utils.int16(apiKey));
    byteBuffer.add(utils.int16(apiVersion ?? 7));
    byteBuffer.add(utils.int32(correlationId));
    byteBuffer.add(utils.nullableString(clientId));
    byteBuffer.addByte(0); // tagged_field

    // byteBuffer.add(utils.nullableString(transactionalId));
    byteBuffer.add(utils.compactNullableString(transactionalId));
    byteBuffer.add(utils.int16(acks));
    byteBuffer.add(utils.int32(timeoutMs));
    // byteBuffer.add(utils.int32(topics.length));
    byteBuffer.add(utils.compactArrayLength(topics.length));

    for (final topic in topics) {
      byteBuffer.add(utils.compactString(topic.topicName));
      // byteBuffer.add(utils.int32(topic.partitions.length));
      byteBuffer.add(utils.compactArrayLength(topic.partitions.length));

      for (Partition partition in topic.partitions) {
        byteBuffer.add(utils.int32(partition.partitionId));

        if (partition.batch == null || partition.batch!.records == null) {
          final message = byteBuffer.toBytes();
          byteBuffer.clear();

          return Uint8List.fromList(
              [...utils.int32(message.length), ...message]);
        }

        // if (partition.batch == null) {
        //   throw Exception(
        //       "The partition ${partition.partitionId} doesn't have the RecordBatch to send "
        //       "but it was included in the topic do be sent");
        // }

        // if (partition.batch!.records == null) {
        //   throw Exception(
        //       "The RecordBatch of the partition ${partition.partitionId} doesn't have the  "
        //       "but it was included in the topic do be sent");
        // }

        byteBuffer.add(utils.recordBatch(partition.batch!.records!));
      }
    }

    final message = byteBuffer.toBytes();
    byteBuffer.clear();

    return Uint8List.fromList([...utils.int32(message.length), ...message]);
  }

  /// Deserialize the ProduceResponse (Version 11)
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    // Read responses array
    final responsesLength = buffer.getInt32(offset);
    offset += 4;

    final responses = <Map<String, dynamic>>[];
    for (int i = 0; i < responsesLength; i++) {
      // Read topic name (STRING)
      final topic = utils.readString(buffer, offset);
      offset += topic.bytesRead;

      // Read partition_responses array
      final partitionResponsesLength = buffer.getInt32(offset);
      offset += 4;

      final partitionResponses = <Map<String, dynamic>>[];
      for (int j = 0; j < partitionResponsesLength; j++) {
        // Read partition (INT32)
        final partition = buffer.getInt32(offset);
        offset += 4;

        // Read error_code (INT16)
        final errorCode = buffer.getInt16(offset);
        offset += 2;

        // Read base_offset (INT64)
        final baseOffset = buffer.getInt64(offset);
        offset += 8;

        // Read log_append_time (INT64)
        final logAppendTime = buffer.getInt64(offset);
        offset += 8;

        // Read log_start_offset (INT64)
        final logStartOffset = buffer.getInt64(offset);
        offset += 8;

        partitionResponses.add({
          'partition': partition,
          'error_code': errorCode,
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

    // Read throttle_time_ms (INT32)
    final throttleTimeMs = buffer.getInt32(offset);
    offset += 4;

    // Read TAG_BUFFER (empty for now)
    utils.readTagBuffer(buffer, offset);

    return {
      'responses': responses,
      'throttle_time_ms': throttleTimeMs,
    };
  }
}
