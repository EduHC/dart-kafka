import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/definitions/message_headers_version.dart';
import 'package:dart_kafka/src/protocol/decoder.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaFindGroupCoordinatorApi {
  final int apiKey = FIND_COORDINATOR;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  Uint8List serialize(
      {required int correlationId,
      required List<String> groups,
      required int apiVersion,
      String? clientId,
      required int coordinatorType}) {
    final byteBuffer = BytesBuilder();

    if (apiVersion > 3) {
      byteBuffer.add(encoder.int8(coordinatorType));
      byteBuffer.add(encoder.compactArrayLength(groups.length));
      for (int i = 0; i < groups.length; i++) {
        byteBuffer.add(encoder.compactString(groups[i]));
      }
      byteBuffer.add(encoder.tagBuffer());
    } else {
      if (apiVersion > 2) {
        byteBuffer.add(encoder.compactString(groups.first));
      } else {
        byteBuffer.add(encoder.string(groups.first));
      }
      if (apiVersion > 0) {
        byteBuffer.add(encoder.int8(coordinatorType));
      }
    }

    final message = byteBuffer.toBytes();
    byteBuffer.clear();

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

  /// Deserialize the JoinGroupResponse
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    int? throttleTimeMs;
    if (apiVersion > 0) {
      throttleTimeMs = buffer.getInt32(offset);
      offset += 4;
    } else {
      throttleTimeMs = null;
    }

    int? errorCode;
    if (apiVersion < 4) {
      errorCode = buffer.getInt16(offset);
      offset += 2;
    } else {
      errorCode = null;
    }

    ({int bytesRead, String? value}) errorMessage;
    if (apiVersion > 0 && apiVersion < 4) {
      if (apiVersion > 2) {
        errorMessage = decoder.readCompactNullableString(buffer, offset);
        offset += errorMessage.bytesRead;
      } else {
        errorMessage = decoder.readString(buffer, offset);
        offset += errorMessage.bytesRead;
      }
    } else {
      errorMessage = (bytesRead: 0, value: null);
    }

    int? nodeId;
    if (apiVersion < 4) {
      nodeId = buffer.getInt32(offset);
      offset += 4;
    } else {
      nodeId = null;
    }

    ({int bytesRead, String? value}) host;
    if (apiVersion < 4) {
      if (apiVersion > 2) {
        host = decoder.readCompactString(buffer, offset);
      } else {
        host = decoder.readString(buffer, offset);
      }
      offset += host.bytesRead;
    } else {
      host = (bytesRead: 0, value: null);
    }

    int? port;
    if (apiVersion < 4) {
      port = buffer.getInt32(offset);
    } else {
      port = null;
    }

    List<Coordinator> coordinators = [];
    if (apiVersion > 3) {
      final coordinatorsLength = decoder.readCompactArrayLength(buffer, offset);
      offset += coordinatorsLength.bytesRead;

      for (int i = 0; i < coordinatorsLength.value; i++) {
        final key = decoder.readCompactString(buffer, offset);
        offset += key.bytesRead;

        final nodeId = buffer.getInt32(offset);
        offset += 4;

        final cHost = decoder.readCompactString(buffer, offset);
        offset += cHost.bytesRead;

        final cPort = buffer.getInt32(offset);
        offset += 4;

        final cErrorCode = buffer.getInt16(offset);
        offset += 2;

        final cErrorMessage = decoder.readCompactNullableString(buffer, offset);
        offset += cErrorMessage.bytesRead;

        final cTaggedField = decoder.readTagBuffer(buffer, offset);
        offset += 1;

        coordinators.add(Coordinator(
            key: key.value,
            nodeId: nodeId,
            host: cHost.value,
            port: cPort,
            errorCode: cErrorCode,
            errorMessage: cErrorMessage.value));
      }
    }

    int? taggedField;
    if (apiVersion > 2) {
      taggedField = buffer.getInt8(offset);
      offset += 1;
    } else {
      taggedField = null;
    }

    return FindGroupCoordinatorResponse(
        throttleTimeMs: throttleTimeMs,
        nodeId: nodeId,
        host: host.value,
        port: port,
        errorCode: errorCode,
        errorMessage: errorMessage.value,
        coordinators: coordinators);
  }
}
