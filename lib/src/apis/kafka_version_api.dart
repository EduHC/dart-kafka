import 'dart:typed_data';

import 'package:dart_kafka/src/models/api_version.dart';
import 'package:dart_kafka/src/models/api_version_response.dart';
import 'package:dart_kafka/src/protocol/utils.dart';
import 'package:dart_kafka/src/protocol/apis.dart';

class KafkaVersionApi {
  final int apiKey;
  final int apiVersion;
  final Utils utils = Utils();

  KafkaVersionApi()
      : apiKey = API_VERSIONS,
        apiVersion = 2;

  /// Serialize the ApiVersionRequest to Byte
  Uint8List serialize({required int correlationId, String? clientId}) {
    final byteBuffer = BytesBuilder();
    byteBuffer.add(utils.int16(apiKey));
    byteBuffer.add(utils.int16(apiVersion));
    byteBuffer.add(utils.int32(correlationId));

    if (clientId != null) {
      final clientIdBytes = clientId!.codeUnits;
      byteBuffer.add(utils.int16(clientIdBytes.length));
      byteBuffer.add(clientIdBytes);
    } else {
      byteBuffer.add(utils.int16(-1));
    }

    Uint8List message = byteBuffer.toBytes();
    return Uint8List.fromList([...utils.int32(message.length), ...message]);
  }

  /// Deserializes the ApiVersionResponse from a byte array.
  dynamic deserialize(Uint8List data) {
    final byteData = ByteData.sublistView(data);

    switch (apiVersion) {
      case 0:
        return _deserialize0(byteData: byteData, messageLength: data.length);
      case 1:
        return _deserialize1(byteData: byteData, messageLength: data.length);

      default:
        return null;
    }
  }

  KafkaApiVersionResponse? _deserialize0(
      {required ByteData byteData, required int messageLength}) {
    int offset = 0;
    final errorCode = byteData.getInt16(offset);
    offset += 2;

    final apiVersions = <ApiVersion>[];
    while (offset < messageLength) {
      final apiKey = byteData.getInt16(offset);
      offset += 2;

      final minVersion = byteData.getInt16(offset);
      offset += 2;

      final maxVersion = byteData.getInt16(offset);
      offset += 2;

      apiVersions.add(ApiVersion(
        apiKey: apiKey,
        minVersion: minVersion,
        maxVersion: maxVersion,
      ));
    }

    final throttleTimeMs = byteData.getInt32(offset);
    offset += 4;

    return KafkaApiVersionResponse(
      version: apiVersion,
      errorCode: errorCode,
      apiVersions: apiVersions,
      throttleTimeMs: throttleTimeMs,
    );
  }

  KafkaApiVersionResponse? _deserialize1(
      {required ByteData byteData, required int messageLength}) {
    int offset = 0;
    final errorCode = byteData.getInt16(offset);
    offset += 2;

    final apiVersionsLength = byteData.getInt32(offset);
    offset += 4;

    final apiVersions = <ApiVersion>[];
    for (int i = 0; i < apiVersionsLength; i++) {
      final apiKey = byteData.getInt16(offset);
      offset += 2;

      final minVersion = byteData.getInt16(offset);
      offset += 2;

      final maxVersion = byteData.getInt16(offset);
      offset += 2;

      apiVersions.add(ApiVersion(
        apiKey: apiKey,
        minVersion: minVersion,
        maxVersion: maxVersion,
      ));
    }

    final throttleTimeMs = byteData.getInt32(offset);
    offset += 4;

    return KafkaApiVersionResponse(
      version: apiVersion,
      errorCode: errorCode,
      apiVersions: apiVersions,
      throttleTimeMs: throttleTimeMs,
    );
  }
}
