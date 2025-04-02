import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/definitions/errors.dart';
import 'package:dart_kafka/src/definitions/message_headers_version.dart';
import 'package:dart_kafka/src/protocol/decoder.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaHeartbeatApi {
  final int apiKey = HEARTBEAT;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Serialize the HeartbeatRequest to bytes
  Uint8List serialize({
    String? clientId,
    String? groupInstanceId,
    required int correlationId,
    required int apiVersion,
    required String groupId,
    required int generationId,
    required String memberId,
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
              apiVersion: apiVersion, apiKey: apiKey),
          messageLength: message.length,
          apiKey: apiKey,
          apiVersion: apiVersion,
          correlationId: correlationId,
          clientId: clientId),
      ...message
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

    int errorCode = buffer.getInt16(offset);
    offset += 2;

    int? taggedField;
    if (apiVersion > 3) {
      taggedField = decoder.readTagBuffer(buffer, offset);
      offset += 1;
    }

    return HeartbeatResponse(
      errorCode: errorCode,
      errorMessage: (ERROR_MAP[errorCode] as Map)['message'],
      throttleTimeMs: throttleTimeMs,
    );
  }
}
