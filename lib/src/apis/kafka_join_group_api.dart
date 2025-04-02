import 'dart:typed_data';
import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/definitions/errors.dart';
import 'package:dart_kafka/src/definitions/message_headers_version.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/protocol/decoder.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaJoinGroupApi {
  final int apiKey = JOIN_GROUP;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Serialize the JoinGroupRequest to bytes
  Uint8List serialize(
      {required int correlationId,
      required String groupId,
      required int sessionTimeoutMs,
      required int rebalanceTimeoutMs,
      required String memberId,
      String? groupInstanceId,
      required String protocolType,
      required List<Protocol> protocols,
      String? reason,
      required int apiVersion,
      String? clientId}) {
    final byteBuffer = BytesBuilder();

    if (apiVersion > 5) {
      byteBuffer.add(encoder.compactString(groupId));
    } else {
      byteBuffer.add(encoder.string(groupId));
    }

    byteBuffer.add(encoder.int32(sessionTimeoutMs));
    byteBuffer.add(encoder.int32(rebalanceTimeoutMs));

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

      BytesBuilder? metadata = BytesBuilder();
      metadata.add(encoder.int16(protocol.metadata.version));

      final topics = protocol.metadata.topics;
      metadata.add(encoder.compactArrayLength(topics.length));
      for (int i = 0; i < topics.length; i++) {
        metadata.add(encoder.compactString(topics[i]));
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
              apiVersion: apiVersion, apiKey: apiKey),
          messageLength: byteBuffer.toBytes().length,
          apiKey: apiKey,
          apiVersion: apiVersion,
          correlationId: correlationId,
          clientId: clientId),
      ...byteBuffer.toBytes()
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

      ({int bytesRead, Uint8List value}) metadata;
      if (apiVersion > 5) {
        metadata = decoder.readCompactBytes(buffer, offset);
        offset += metadata.bytesRead;
      } else {
        metadata = decoder.readBytes(buffer, offset);
        offset += metadata.bytesRead;
      }

      if (apiVersion > 5) {
        final mTaggedField = buffer.getInt8(offset);
        offset += 1;
      }

      members.add(Member(
          memberId: memberId.value!,
          metadata: metadata.value,
          groupInstanceId: groupInstanceId.value));
    }

    if (apiVersion > 5) {
      final taggedField = decoder.readTagBuffer(buffer, offset);
      offset += 1;
    }

    return JoinGroupResponse(
      throttleTimeMs: throttleTimeMs,
      errorCode: errorCode,
      errorMessage: (ERROR_MAP[errorCode] as Map)['message'],
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
