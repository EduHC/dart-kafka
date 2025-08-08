import 'dart:typed_data';

import '../../common/error.dart';
import '../../common/member.dart';
import '../../protocol/decoder.dart';
import '../../protocol/definition/api.dart';
import '../../protocol/definition/message_header_version.dart';
import '../../protocol/encoder.dart';
import '../../protocol/utils.dart';
import 'leave_group_response.dart';

class KafkaLeaveGroupApi {
  final int apiKey = LEAVE_GROUP;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Serialize the LeaveGroupRequest to bytes
  Uint8List serialize({
    required String groupId,
    required List<Member> members,
    required int correlationId,
    required int apiVersion,
    String? clientId,
    String? memberId,
  }) {
    BytesBuilder? byteBuffer = BytesBuilder();

    if (apiVersion > 2) {
      if (apiVersion > 3) {
        byteBuffer
          ..add(encoder.compactString(groupId))
          ..add(encoder.compactArrayLength(members.length));
      } else {
        byteBuffer
          ..add(encoder.string(groupId))
          ..add(encoder.int32(members.length));
      }

      for (final Member member in members) {
        if (apiVersion > 3) {
          byteBuffer.add(encoder.compactString(member.memberId));
        } else {
          byteBuffer.add(encoder.string(member.memberId));
        }

        if (apiVersion > 3) {
          byteBuffer.add(encoder.compactNullableString(member.groupInstanceId));
        } else {
          byteBuffer.add(encoder.nullableString(member.groupInstanceId));
        }
        if (apiVersion > 4) {
          byteBuffer.add(encoder.compactNullableString(member.reason));
        }

        if (apiVersion > 3) {
          byteBuffer.add(encoder.tagBuffer());
        }
      }

      if (apiVersion > 3) {
        byteBuffer.add(encoder.tagBuffer());
      }
    } else {
      if (memberId == null) {
        throw Exception(
            'MemberId passed as null to LeaveGroupRequest version < 3 and '
            'is a not null field.');
      }
      byteBuffer
        ..add(encoder.string(groupId))
        ..add(encoder.string(memberId));
    }

    final message = byteBuffer.toBytes();
    byteBuffer.clear();
    byteBuffer = null;

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

  /// Deserialize the LeaveGroupResponse
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    int? throttleTimeMs;
    if (apiVersion > 1) {
      throttleTimeMs = buffer.getInt32(offset);
      offset += 4;
    }

    final int errorCode = buffer.getInt16(offset);
    offset += 2;

    ({int bytesRead, int value}) arrayLength;
    if (apiVersion > 3) {
      arrayLength = decoder.readCompactArrayLength(buffer, offset);
      offset += arrayLength.bytesRead;
    } else {
      final int len = buffer.getInt32(offset);
      offset += 4;
      arrayLength = (bytesRead: 4, value: len);
    }

    final List<Member> members = [];
    for (int i = 0; i < arrayLength.value; i++) {
      ({int bytesRead, String value}) memberId;
      if (apiVersion > 3) {
        memberId = decoder.readCompactString(buffer, offset);
      } else {
        memberId = decoder.readString(buffer, offset);
      }
      offset += memberId.bytesRead;

      ({int bytesRead, String? value}) groupInstanceId;
      if (apiVersion > 3) {
        groupInstanceId = decoder.readCompactNullableString(buffer, offset);
      } else {
        groupInstanceId = decoder.readNullableString(buffer, offset);
      }
      offset += groupInstanceId.bytesRead;

      final int errorCode = buffer.getInt16(offset);
      offset += 2;

      members.add(
        Member(
          memberId: memberId.value,
          groupInstanceId: groupInstanceId.value,
          errorCode: errorCode,
          errorMessage: (ERROR_MAP[errorCode]! as Map)['message'],
        ),
      );
    }

    return LeaveGroupResponse(
      errorCode: errorCode,
      errorMessage: (ERROR_MAP[errorCode]! as Map)['message'],
      members: members,
      throttleTimeMs: throttleTimeMs,
    );
  }
}
