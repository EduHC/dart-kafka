import 'dart:typed_data';

import '../../dart_kafka.dart';
import '../definitions/apis.dart';
import '../definitions/errors.dart';
import '../definitions/message_headers_version.dart';
import '../models/responses/offset_commit_response.dart';
import '../protocol/decoder.dart';
import '../protocol/endocer.dart';
import '../protocol/utils.dart';

class KafkaOffsetCommitApi {
  final int apiKey = OFFSET_COMMIT;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Method to serialize the OffsetCommmitRequest
  Uint8List serialize({
    required int correlationId,
    required int apiVersion,
    required String groupId,
    required int generationIdOrMemberEpoch,
    required String memberId,
    required List<OffsetCommitTopic> topics,
    String? clientId,
    String? groupInstanceId,
  }) {
    BytesBuilder? buffer = BytesBuilder()
      ..add(encoder.compactString(groupId))
      ..add(encoder.int32(generationIdOrMemberEpoch))
      ..add(encoder.compactString(memberId))
      ..add(encoder.compactNullableString(groupInstanceId))
      ..add(encoder.compactArrayLength(topics.length));

    for (final OffsetCommitTopic topic in topics) {
      buffer
        ..add(encoder.compactString(topic.name))
        ..add(encoder.compactArrayLength(topic.partitions.length));

      for (final OffsetCommitPartition partition in topic.partitions) {
        buffer
          ..add(encoder.int32(partition.id))
          ..add(encoder.int64(partition.commitedOffset))
          ..add(encoder.int32(partition.commitedLeaderEpoch))
          ..add(encoder.compactNullableString(partition.commitedMetadata))
          ..add(encoder.tagBuffer());
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
      ...message,
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

    final List<ResOffsetCommitTopic> topics = [];
    for (int i = 0; i < topicsLen.value; i++) {
      final name = decoder.readCompactString(buffer, offset);
      offset += name.bytesRead;

      final partitionsLen = decoder.readCompactArrayLength(buffer, offset);
      offset += partitionsLen.bytesRead;

      final List<ResOffsetCommitPartition> partitions = [];
      for (int j = 0; j < partitionsLen.value; j++) {
        final id = buffer.getInt32(offset);
        offset += 4;

        final errorCode = buffer.getInt16(offset);
        offset += 2;

        // final pTaggedField = decoder.readTagBuffer(buffer, offset);
        offset += 1;

        partitions.add(
          ResOffsetCommitPartition(
            id: id,
            errorCode: errorCode,
            errorMessage: (ERROR_MAP[errorCode]! as Map)['message'],
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
