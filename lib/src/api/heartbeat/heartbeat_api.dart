import 'dart:typed_data';

import '../../common/error.dart';
import '../../protocol/decoder.dart';
import '../../protocol/definition/api.dart';
import '../../protocol/definition/message_header_version.dart';
import '../../protocol/encoder.dart';
import '../../protocol/utils.dart';
import 'heartbeat_response.dart';

class KafkaHeartbeatApi {
  final int apiKey = HEARTBEAT;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Serialize the HeartbeatRequest to bytes
  Uint8List serialize({
    required int correlationId,
    required int apiVersion,
    required String groupId,
    required int generationId,
    required String memberId,
    String? clientId,
    String? groupInstanceId,
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

    if (apiVersion > 3) {
      byteBuffer.add(encoder.tagBuffer());
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

  /// Deserialize the HeartbeatResponse
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    int? throttleTimeMs;
    if (apiVersion > 0) {
      buffer.getInt32(offset);
      offset += 4;
    }

    final int errorCode = buffer.getInt16(offset);
    offset += 2;

    int? taggedField;
    if (apiVersion > 3) {
      taggedField = decoder.readTagBuffer(buffer, offset);
      offset += 1;
    }

    return HeartbeatResponse(
      errorCode: errorCode,
      errorMessage: (ERROR_MAP[errorCode]! as Map)['message'],
      throttleTimeMs: throttleTimeMs,
    );
  }
}
