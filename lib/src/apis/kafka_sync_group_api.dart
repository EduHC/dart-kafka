import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/definitions/errors.dart';
import 'package:dart_kafka/src/definitions/message_headers_version.dart';
import 'package:dart_kafka/src/models/responses/sync_group_response.dart';
import 'package:dart_kafka/src/protocol/decoder.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaSyncGroupApi {
  final int apiKey = SYNC_GROUP;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Serialize the SyncGroup to bytes
  Uint8List serialize({
    String? clientId,
    String? groupInstanceId,
    String? protocolType,
    String? protocolName,
    required int correlationId,
    required int apiVersion,
    required String groupId,
    required int generationId,
    required String memberId,
    required List<AssignmentSyncGroup> assignments,
  }) {
    BytesBuilder? byteBuffer = BytesBuilder();

    if (apiVersion > 3) {
      byteBuffer.add(encoder.compactString(groupId));
    } else {
      byteBuffer.add(encoder.string(groupId));
    }

    byteBuffer.add(encoder.int32(generationId));

    if (apiVersion > 3) {
      byteBuffer.add(encoder.compactString(memberId));
    } else {
      byteBuffer.add(encoder.string(memberId));
    }

    if (apiVersion > 2) {
      if (apiVersion > 3) {
        byteBuffer.add(encoder.compactNullableString(groupInstanceId));
      } else {
        byteBuffer.add(encoder.nullableString(groupInstanceId));
      }
    }

    if (apiVersion > 4) {
      byteBuffer.add(encoder.compactNullableString(protocolType));
    }

    if (apiVersion > 4) {
      byteBuffer.add(encoder.compactNullableString(protocolName));
    }

    if (apiVersion > 3) {
      byteBuffer.add(encoder.compactArrayLength(assignments.length));
    } else {
      byteBuffer.add(encoder.int32(assignments.length));
    }

    for (AssignmentSyncGroup assignmentSync in assignments) {
      if (apiVersion > 3) {
        byteBuffer.add(encoder.compactString(assignmentSync.memberId));
      } else {
        byteBuffer.add(encoder.string(assignmentSync.memberId));
      }

      // here add the assignmentSync.assignment
      BytesBuilder? assignmentBuffer = BytesBuilder();
      if (assignmentSync.assignment != null) {
        assignmentBuffer.add(encoder.int16(assignmentSync.assignment!.version));
        assignmentBuffer.add(
          encoder.int32(assignmentSync.assignment!.topics.length),
        );

        for (AssignmentTopicMetadata topic in assignmentSync.assignment!.topics) {
          assignmentBuffer.add(encoder.string(topic.topicName));
          assignmentBuffer.add(encoder.int32(topic.partitions.length));

          for (int partition in topic.partitions) {
            assignmentBuffer.add(encoder.int32(partition));
          }
        }

        assignmentBuffer.add(
          encoder.int32((assignmentSync.assignment!.userData?.length) ?? 0),
        );
      }

      if (apiVersion > 3) {
        byteBuffer.add(
          encoder.compactArrayLength(assignmentBuffer.toBytes().length),
        );
      } else {
        byteBuffer.add(encoder.int32(assignmentBuffer.toBytes().length));
      }
      byteBuffer.add(assignmentBuffer.toBytes());

      if (apiVersion > 3) {
        byteBuffer.add(encoder.tagBuffer());
      }
    }

    if (apiVersion > 3) {
      byteBuffer.add(encoder.tagBuffer());
    }

    final message = byteBuffer.toBytes();
    byteBuffer.clear();
    byteBuffer = null;

    return Uint8List.fromList([
      ...encoder.writeMessageHeader(
          version: MessageHeaderVersion.requestHeaderVersion(
              apiVersion: apiVersion, apiKey: apiKey),
          messageLength: message.length,
          apiKey: apiKey,
          apiVersion: apiVersion,
          correlationId: correlationId,
          clientId: clientId),
      ...message
    ]);
  }

  /// Deserialize the SyncGroupResponse
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    int? throttleTimeMs;
    if (apiVersion > 0) {
      throttleTimeMs = buffer.getInt32(offset);
      offset += 4;
    }

    final int errorCode = buffer.getInt16(offset);
    offset += 2;

    ({int bytesRead, String? value}) protocolType = (bytesRead: 0, value: null);
    ({int bytesRead, String? value}) protocolName = (bytesRead: 0, value: null);
    if (apiVersion > 4) {
      protocolType = decoder.readCompactNullableString(buffer, offset);
      offset += protocolType.bytesRead;

      protocolName = decoder.readCompactNullableString(buffer, offset);
      offset += protocolName.bytesRead;
    }

    ({int bytesRead, int value}) length;
    if (apiVersion > 3) {
      length = decoder.readCompactArrayLength(buffer, offset);
      offset += length.bytesRead;
    } else {
      final int len = buffer.getInt32(offset);
      offset += 4;
      length = (bytesRead: 4, value: len);
    }

    int? version;
    int? topicsLen;
    List<AssignmentTopicMetadata> topics = [];
    Assignment? assignment;
    if (length.value > 0) {
      version = buffer.getInt16(offset);
      offset += 2;

      topicsLen = buffer.getInt32(offset);
      offset += 4;

      for (int i = 0; i < topicsLen; i++) {
        final topicName = decoder.readString(buffer, offset);
        offset += topicName.bytesRead;

        final int partitionsLen = buffer.getInt32(offset);
        offset += 4;

        final List<int> partitions = [];
        for (int j = 0; j < partitionsLen; j++) {
          partitions.add(buffer.getInt32(offset));
          offset += 4;
        }

        topics.add(AssignmentTopicMetadata(
          topicName: topicName.value,
          partitions: partitions,
        ));
      }

      assignment = Assignment(
        version: version,
        topics: topics,
      );
    }

    int? taggedField;
    if (apiVersion > 3) {
      taggedField = buffer.getInt8(offset);
      offset += 1;
    }

    return SyncGroupResponse(
      errorCode: errorCode,
      errorMessage: (ERROR_MAP[errorCode] as Map)['message'],
      assignment: assignment,
      protocolName: protocolName.value,
      protocolType: protocolType.value,
      throttleTimeMs: throttleTimeMs,
    );
  }
}
