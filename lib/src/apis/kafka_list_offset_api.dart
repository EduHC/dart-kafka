import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/protocol/decoder.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/definitions/errors.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaListOffsetApi {
  final int apiKey = LIST_OFFSETS;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Method to serialize the ListOffsetsRequest
  Uint8List serialize({
    required int correlationId,
    required int apiVersion,
    String? clientId,
    required int replicaId,
    required int isolationLevel,
    required List<Topic> topics,
    required int leaderEpoch,
    required int limit,
    required DateTime timestamp,
  }) {
    final byteBuffer = BytesBuilder();

    byteBuffer.add(encoder.int32(replicaId));
    if (apiVersion > 1) {
      byteBuffer.add(encoder.int8(isolationLevel));
    }
    byteBuffer.add(encoder.compactArrayLength(topics.length));

    for (Topic topic in topics) {
      if (apiVersion > 5) {
        byteBuffer.add(encoder.compactString(topic.topicName));
      } else {
        byteBuffer.add(encoder.string(topic.topicName));
      }
      byteBuffer.add(encoder.compactArrayLength(topic.partitions?.length ?? 0));

      for (Partition partition in topic.partitions ?? []) {
        byteBuffer.add(encoder.int32(partition.id));
        if (apiVersion > 3) {
          byteBuffer.add(encoder.int32(leaderEpoch));
        }
        byteBuffer.add(encoder.int64(timestamp.microsecondsSinceEpoch));
        if (apiVersion == 0) {
          byteBuffer.add(encoder.int32(limit));
        }
        if (apiVersion > 5) {
          byteBuffer.add(encoder.int8(0));
        }
      }

      if (apiVersion > 5) byteBuffer.add(encoder.int8(0));
    }

    if (apiVersion > 5) byteBuffer.add(encoder.int8(0));

    final message = byteBuffer.toBytes();
    byteBuffer.clear();

    return Uint8List.fromList([
      ...encoder.writeMessageHeader(
          apiKey: apiKey,
          apiVersion: apiVersion,
          clientId: clientId,
          correlationId: correlationId,
          messageLength: message.length,
          version: apiVersion > 6 ? 2 : 1),
      ...message
    ]);
  }

  /// Method to deserialize the ListOffsetsResponse
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    final int throttleTimeMs = buffer.getInt32(offset);
    offset += 4;

    final topicsLength = decoder.readCompactArrayLength(buffer, offset);
    offset += topicsLength.bytesRead;

    List<Topic> topics = [];
    for (int i = 0; i < topicsLength.value; i++) {
      final topicName = decoder.readCompactString(buffer, offset);
      offset += topicName.bytesRead;

      final partitionsLength = decoder.readCompactArrayLength(buffer, offset);
      offset += partitionsLength.bytesRead;

      List<Partition> partitions = [];
      for (int j = 0; j < partitionsLength.value; j++) {
        final int id = buffer.getInt32(offset);
        offset += 4;

        final int errorCode = buffer.getInt16(offset);
        offset += 2;

        final int timestamp = buffer.getInt64(offset);
        offset += 8;

        final int pOffset = buffer.getInt64(offset);
        offset += 8;

        final int leaderEpoch = buffer.getInt32(offset);
        offset += 4;

        final int pTaggedField = buffer.getInt8(offset);
        offset += 1;

        partitions.add(
          Partition(
            id: id,
            errorCode: errorCode,
            errorMessage: (ERROR_MAP[errorCode] as Map)['message'],
            timestamp: timestamp,
            offset: pOffset,
            leaderEpoch: leaderEpoch,
          ),
        );
      }

      final int tTaggedBuffer = buffer.getInt8(offset);
      offset += 1;

      topics.add(Topic(topicName: topicName.value, partitions: partitions));
    }

    final int taggedBuffer = buffer.getInt8(offset);
    offset += 1;

    return ListOffsetResponse(throttleTimeMs: throttleTimeMs, topics: topics);
  }
}
