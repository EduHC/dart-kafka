import 'dart:typed_data';

import '../../dart_kafka.dart';
import '../definitions/apis.dart';
import '../definitions/errors.dart';
import '../definitions/message_headers_version.dart';
import '../protocol/decoder.dart';
import '../protocol/endocer.dart';
import '../protocol/utils.dart';

class KafkaJoinGroupApi {
  final int apiKey = JOIN_GROUP;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Serialize the JoinGroupRequest to bytes
  Uint8List serialize({
    required int correlationId,
    required String groupId,
    required int sessionTimeoutMs,
    required int rebalanceTimeoutMs,
    required String memberId,
    required String protocolType,
    required List<Protocol> protocols,
    required int apiVersion,
    String? groupInstanceId,
    String? reason,
    String? clientId,
  }) {
    final byteBuffer = BytesBuilder();

    if (apiVersion > 5) {
      byteBuffer.add(encoder.compactString(groupId));
    } else {
      byteBuffer.add(encoder.string(groupId));
    }

    byteBuffer
      ..add(encoder.int32(sessionTimeoutMs))
      ..add(encoder.int32(rebalanceTimeoutMs));

    if (apiVersion > 5) {
      byteBuffer.add(encoder.compactString(memberId));
    } else {
      byteBuffer.add(encoder.string(memberId));
    }

    if (apiVersion > 4) {
      if (apiVersion > 5) {
        byteBuffer.add(encoder.compactNullableString(groupInstanceId));
      } else {
        byteBuffer.add(encoder.nullableString(groupInstanceId));
      }
    }

    if (apiVersion > 5) {
      byteBuffer.add(encoder.compactString(protocolType));
    } else {
      byteBuffer.add(encoder.string(protocolType));
    }

    if (apiVersion > 5) {
      byteBuffer.add(encoder.compactArrayLength(protocols.length));
    } else {
      byteBuffer.add(encoder.int32(protocols.length));
    }

    for (final protocol in protocols) {
      if (apiVersion > 5) {
        byteBuffer.add(encoder.compactString(protocol.name));
      } else {
        byteBuffer.add(encoder.string(protocol.name));
      }

      final BytesBuilder metadata = BytesBuilder()
        ..add(encoder.int16(protocol.metadata.version));

      final topics = protocol.metadata.topics;
      metadata.add(encoder.int32(topics.length));
      for (int i = 0; i < topics.length; i++) {
        metadata.add(encoder.string(topics[i]));
      }
      metadata.add(encoder.compactBytes(Uint8List.fromList([])));

      final metadataMsg = metadata.toBytes();
      if (apiVersion > 5) {
        byteBuffer.add(encoder.compactBytes(metadataMsg));
      } else {
        byteBuffer.add(encoder.bytes(metadataMsg));
      }

      metadata.clear();

      if (apiVersion > 5) {
        byteBuffer.add(encoder.tagBuffer());
      }
    }

    if (apiVersion > 7) {
      byteBuffer.add(encoder.compactNullableString(reason));
    }

    if (apiVersion > 5) {
      byteBuffer.add(encoder.tagBuffer());
    }

    return Uint8List.fromList([
      ...encoder.writeMessageHeader(
        version: MessageHeaderVersion.requestHeaderVersion(
          apiVersion: apiVersion,
          apiKey: apiKey,
        ),
        messageLength: byteBuffer.toBytes().length,
        apiKey: apiKey,
        apiVersion: apiVersion,
        correlationId: correlationId,
        clientId: clientId,
      ),
      ...byteBuffer.toBytes(),
    ]);
  }

  /// Deserialize the JoinGroupResponse
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    final throttleTimeMs = buffer.getInt32(offset);
    offset += 4;

    final errorCode = buffer.getInt16(offset);
    offset += 2;

    final generationId = buffer.getInt32(offset);
    offset += 4;

    ({int bytesRead, String? value}) protocolType;
    if (apiVersion > 6) {
      protocolType = decoder.readCompactNullableString(buffer, offset);
      offset += protocolType.bytesRead;
    } else {
      protocolType = (bytesRead: 0, value: null);
    }

    ({int bytesRead, String? value}) protocolName;
    if (apiVersion > 6) {
      protocolName = decoder.readCompactNullableString(buffer, offset);
      offset += protocolName.bytesRead;
    } else if (apiVersion > 5) {
      protocolName = decoder.readCompactString(buffer, offset);
      offset += protocolName.bytesRead;
    } else {
      protocolName = decoder.readString(buffer, offset);
      offset += protocolName.bytesRead;
    }

    ({int bytesRead, String? value}) leader;
    if (apiVersion > 5) {
      leader = decoder.readCompactString(buffer, offset);
      offset += leader.bytesRead;
    } else {
      leader = decoder.readString(buffer, offset);
      offset += leader.bytesRead;
    }

    bool skipAssignment;
    if (apiVersion > 8) {
      skipAssignment = buffer.getInt8(offset) != 0;
      offset += 1;
    } else {
      skipAssignment = false;
    }

    ({int bytesRead, String? value}) memberId;
    if (apiVersion > 5) {
      memberId = decoder.readCompactString(buffer, offset);
      offset += memberId.bytesRead;
    } else {
      memberId = decoder.readString(buffer, offset);
      offset += memberId.bytesRead;
    }

    final membersLength = decoder.readCompactArrayLength(buffer, offset);
    offset += membersLength.bytesRead;

    final members = <Member>[];
    for (int i = 0; i < membersLength.value; i++) {
      ({int bytesRead, String? value}) memberId;
      if (apiVersion > 5) {
        memberId = decoder.readCompactString(buffer, offset);
        offset += memberId.bytesRead;
      } else {
        memberId = decoder.readString(buffer, offset);
        offset += memberId.bytesRead;
      }

      ({int bytesRead, String? value}) groupInstanceId;
      if (apiVersion < 5) {
        groupInstanceId = (bytesRead: 0, value: null);
      } else if (apiVersion > 5) {
        groupInstanceId = decoder.readCompactNullableString(buffer, offset);
        offset += groupInstanceId.bytesRead;
      } else {
        groupInstanceId = decoder.readNullableString(buffer, offset);
        offset += groupInstanceId.bytesRead;
      }

      ({int bytesRead, int value}) metadataLen;
      if (apiVersion > 5) {
        metadataLen = decoder.readCompactArrayLength(buffer, offset);
      } else {
        final int len = buffer.getInt32(offset);
        metadataLen = (bytesRead: 4, value: len);
      }
      offset += metadataLen.bytesRead;

      MemberMetadata? metadata;
      if (metadataLen.value > 0) {
        final int version = buffer.getInt16(offset);
        offset += 2;

        final int topicsLen = buffer.getInt32(offset);
        offset += 4;

        final List<String> topics = [];
        for (int j = 0; j < topicsLen; j++) {
          final topic = decoder.readString(buffer, offset);
          offset += topic.bytesRead;
          topics.add(topic.value);
        }

        metadata = MemberMetadata(version: version, topics: topics);
      }

      if (apiVersion > 5) {
        final mTaggedField = buffer.getInt8(offset);
        offset += 1;
      }

      members.add(
        Member(
          memberId: memberId.value!,
          metadata: metadata,
          groupInstanceId: groupInstanceId.value,
        ),
      );
    }

    if (apiVersion > 5) {
      final taggedField = decoder.readTagBuffer(buffer, offset);
      offset += 1;
    }

    return JoinGroupResponse(
      throttleTimeMs: throttleTimeMs,
      errorCode: errorCode,
      errorMessage: (ERROR_MAP[errorCode]! as Map)['message'],
      generationId: generationId,
      leader: leader.value!,
      memberId: memberId.value!,
      members: members,
      protocolName: protocolName.value,
      protocolType: protocolType.value,
      skipAssignment: skipAssignment,
    );
  }
}
