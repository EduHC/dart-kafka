import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/definitions/errors.dart';
import 'package:dart_kafka/src/definitions/message_headers_version.dart';
import 'package:dart_kafka/src/models/responses/offset_commit_response.dart';
import 'package:dart_kafka/src/protocol/decoder.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaOffsetCommitApi {
  final int apiKey = OFFSET_COMMIT;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Method to serialize the OffsetCommmitRequest
  Uint8List serialize({
    String? clientId,
    String? groupInstanceId,
    required int correlationId,
    required int apiVersion,
    required String groupId,
    required int generationIdOrMemberEpoch,
    required String memberId,
    required List<OffsetCommitTopic> topics,
  }) {
    BytesBuilder? buffer = BytesBuilder();

    buffer.add(encoder.compactString(groupId));
    buffer.add(encoder.int32(generationIdOrMemberEpoch));
    buffer.add(encoder.compactString(memberId));
    buffer.add(encoder.compactNullableString(groupInstanceId));
    buffer.add(encoder.compactArrayLength(topics.length));

    for (OffsetCommitTopic topic in topics) {
      buffer.add(encoder.compactString(topic.name));
      buffer.add(encoder.compactArrayLength(topic.partitions.length));

      for (OffsetCommitPartition partition in topic.partitions) {
        buffer.add(encoder.int32(partition.id));
        buffer.add(encoder.int64(partition.commitedOffset));
        buffer.add(encoder.int32(partition.commitedLeaderEpoch));
        buffer.add(encoder.compactNullableString(partition.commitedMetadata));
        buffer.add(encoder.tagBuffer());
      }

      buffer.add(encoder.tagBuffer());
    }

    buffer.add(encoder.tagBuffer());

    final message = buffer.toBytes();
    buffer.clear();
    buffer = null;

    return Uint8List.fromList([
      ...encoder.writeMessageHeader(
        version: MessageHeaderVersion.requestHeaderVersion(
          apiVersion: apiVersion,
          apiKey: apiKey,
        ),
        messageLength: message.length,
        apiKey: apiKey,
        apiVersion: apiVersion,
        correlationId: correlationId,
        clientId: clientId,
      ),
      ...message
    ]);
  }

  /// Method to deserialize the OffsetCommitResponse
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    final int throttleTimeMs = buffer.getInt32(offset);
    offset += 4;

    final topicsLen = decoder.readCompactArrayLength(buffer, offset);
    offset += topicsLen.bytesRead;

    List<ResOffsetCommitTopic> topics = [];
    for (int i = 0; i < topicsLen.value; i++) {
      final name = decoder.readCompactString(buffer, offset);
      offset += name.bytesRead;

      final partitionsLen = decoder.readCompactArrayLength(buffer, offset);
      offset += partitionsLen.bytesRead;

      List<ResOffsetCommitPartition> partitions = [];
      for (int j = 0; j < partitionsLen.value; j++) {
        final id = buffer.getInt32(offset);
        offset += 4;

        final errorCode = buffer.getInt16(offset);
        offset += 2;

        final pTaggedField = decoder.readTagBuffer(buffer, offset);
        offset += 1;

        partitions.add(
          ResOffsetCommitPartition(
            id: id,
            errorCode: errorCode,
            errorMessage: (ERROR_MAP[errorCode] as Map)['message'],
          ),
        );
      }

      final tTaggedField = decoder.readTagBuffer(buffer, offset);
      offset += 1;

      topics.add(
        ResOffsetCommitTopic(
          name: name.value,
          partitions: partitions,
        ),
      );
    }

    final taggedField = decoder.readTagBuffer(buffer, offset);
    offset += 1;

    return OffsetCommitResponse(throttleTimeMs: throttleTimeMs, topics: topics);
  }
}
