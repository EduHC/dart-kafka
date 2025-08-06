import 'dart:typed_data';

import '../../dart_kafka.dart';
import '../definitions/apis.dart';
import '../definitions/errors.dart';
import '../definitions/message_headers_version.dart';
import '../protocol/decoder.dart';
import '../protocol/endocer.dart';
import '../protocol/utils.dart';

class KafkaOffsetFetchApi {
  final int apiKey = OFFSET_FETCH;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Method to serialize the OffsetFetchRequest
  Uint8List serialize({
    required int correlationId,
    required int apiVersion,
    required List<RequestGroup> groups,
    required bool requireStable,
    String? clientId,
  }) {
    BytesBuilder? buffer = BytesBuilder()
      ..add(encoder.compactArrayLength(groups.length));
    for (final RequestGroup group in groups) {
      buffer
        ..add(encoder.compactString(group.groupId))
        ..add(encoder.compactNullableString(group.memberId))
        ..add(encoder.int32(group.memberEpoch))
        ..add(encoder.compactArrayLength(group.topics.length));

      for (final GroupTopic topic in group.topics) {
        buffer
          ..add(encoder.compactString(topic.name))
          ..add(encoder.compactArrayLength(topic.partitions.length));

        for (final int id in topic.partitions) {
          buffer.add(encoder.int32(id));
        }

        buffer.add(encoder.tagBuffer());
      }

      buffer.add(encoder.tagBuffer());
    }

    buffer
      ..add(encoder.boolean(value: requireStable))
      ..add(encoder.tagBuffer());

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

  /// Method to deserialize the OffsetFetchRequest
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    int? throttleTimeMs;
    if (apiVersion > 2) {
      throttleTimeMs = buffer.getInt32(offset);
      offset += 4;
    }

    final List<ResponseGroup> groups = [];
    final List<OffsetFetchTopic> topics = [];
    final List<OffsetFetchPartition> partitions = [];

    if (apiVersion < 8) {
      ({int bytesRead, int value}) topicsLen;
      if (apiVersion > 5) {
        topicsLen = decoder.readCompactArrayLength(buffer, offset);
      } else {
        topicsLen = (bytesRead: 4, value: buffer.getInt32(offset));
      }
      offset += topicsLen.bytesRead;

      for (int i = 0; i < topicsLen.value; i++) {
        ({int bytesRead, String value}) topicName;
        if (apiVersion > 5) {
          topicName = decoder.readCompactString(buffer, offset);
        } else {
          topicName = decoder.readString(buffer, offset);
        }
        offset += topicName.bytesRead;

        ({int bytesRead, int value}) partitionsLen;
        if (apiVersion > 5) {
          partitionsLen = decoder.readCompactArrayLength(buffer, offset);
        } else {
          final int len = buffer.getInt32(offset);
          partitionsLen = (bytesRead: 4, value: len);
        }
        offset += partitionsLen.bytesRead;

        for (int j = 0; j < partitionsLen.value; j++) {
          final int id = buffer.getInt32(offset);
          offset += 4;

          final int commitedOffset = buffer.getInt64(offset);
          offset += 8;

          int commitedLeaderEpoch;
          if (apiVersion > 4) {
            commitedLeaderEpoch = buffer.getInt32(offset);
            offset += 4;
          } else {
            commitedLeaderEpoch = -1;
          }

          ({int bytesRead, String? value}) metadata;
          if (apiVersion > 5) {
            metadata = decoder.readCompactNullableString(buffer, offset);
          } else {
            metadata = decoder.readNullableString(buffer, offset);
          }
          offset += metadata.bytesRead;

          final int errorCode = buffer.getInt16(offset);
          offset += 2;

          int? pTaggedField;
          if (apiVersion > 5) {
            pTaggedField = decoder.readTagBuffer(buffer, offset);
            offset += 1;
          }

          partitions.add(
            OffsetFetchPartition(
              id: id,
              commitedOffset: commitedOffset,
              commitedLeaderEpoch: commitedLeaderEpoch,
              errorCode: errorCode,
              errorMessage: (ERROR_MAP[errorCode]! as Map)['message'],
            ),
          );
        }

        int errorCode;
        if (apiVersion > 3 && apiVersion < 8) {
          errorCode = buffer.getInt16(offset);
          offset += 2;
        } else {
          errorCode = 0;
        }

        topics.add(
          OffsetFetchTopic(
            name: topicName.value,
            partitions: partitions,
            errorCode: errorCode,
            errorMessage: (ERROR_MAP[errorCode]! as Map)['message'],
          ),
        );
      }
    } else {
      final groupsLen = decoder.readCompactArrayLength(buffer, offset);
      offset += groupsLen.bytesRead;

      for (int i = 0; i < groupsLen.value; i++) {
        final groupId = decoder.readCompactString(buffer, offset);
        offset += groupId.bytesRead;

        // offsetTopic
        final topicsLen = decoder.readCompactArrayLength(buffer, offset);
        offset += topicsLen.bytesRead;

        for (int i = 0; i < topicsLen.value; i++) {
          ({int bytesRead, String value}) topicName;
          if (apiVersion > 5) {
            topicName = decoder.readCompactString(buffer, offset);
          } else {
            topicName = decoder.readString(buffer, offset);
          }
          offset += topicName.bytesRead;

          ({int bytesRead, int value}) partitionsLen;
          if (apiVersion > 5) {
            partitionsLen = decoder.readCompactArrayLength(buffer, offset);
          } else {
            final int len = buffer.getInt32(offset);
            partitionsLen = (bytesRead: 4, value: len);
          }
          offset += partitionsLen.bytesRead;

          for (int j = 0; j < partitionsLen.value; j++) {
            final int id = buffer.getInt32(offset);
            offset += 4;

            final int commitedOffset = buffer.getInt64(offset);
            offset += 8;

            int commitedLeaderEpoch;
            if (apiVersion > 4) {
              commitedLeaderEpoch = buffer.getInt32(offset);
              offset += 4;
            } else {
              commitedLeaderEpoch = -1;
            }

            ({int bytesRead, String? value}) metadata;
            if (apiVersion > 5) {
              metadata = decoder.readCompactNullableString(buffer, offset);
            } else {
              metadata = decoder.readNullableString(buffer, offset);
            }
            offset += metadata.bytesRead;

            final int errorCode = buffer.getInt16(offset);
            offset += 2;

            final int pTaggedField = decoder.readTagBuffer(buffer, offset);
            offset += 1;

            partitions.add(
              OffsetFetchPartition(
                id: id,
                commitedOffset: commitedOffset,
                commitedLeaderEpoch: commitedLeaderEpoch,
                errorCode: errorCode,
                errorMessage: (ERROR_MAP[errorCode]! as Map)['message'],
              ),
            );
          }

          final int tTaggedField = decoder.readTagBuffer(buffer, offset);
          offset += 1;

          topics.add(
            OffsetFetchTopic(name: topicName.value, partitions: partitions),
          );
        }

        final errorCode = buffer.getInt16(offset);
        offset += 2;

        final int gTaggedField = decoder.readTagBuffer(buffer, offset);
        offset += 1;

        groups.add(
          ResponseGroup(
            name: groupId.value,
            topics: topics,
            errorCode: errorCode,
          ),
        );
      }

      final int taggedField = decoder.readTagBuffer(buffer, offset);
      offset += 1;
    }

    return OffsetFetchResponse(
      throttleTimeMs: throttleTimeMs,
      groups: groups,
      topics: apiVersion < 8 ? topics : null,
    );
  }
}
