import 'dart:typed_data';

import 'package:dart_kafka/src/models/api_version.dart';
import 'package:dart_kafka/src/models/api_version_response.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaVersionApi {
  final int apiKey;
  final int apiVersion;
  final Utils utils = Utils();

  KafkaVersionApi()
      : apiKey = 18,
        apiVersion = 0;

  Uint8List serialize(int correlationId, String? clientId) {
    final byteBuffer = BytesBuilder();
    byteBuffer.add(utils.int16(apiKey));
    byteBuffer.add(utils.int16(apiVersion));
    byteBuffer.add(utils.int32(correlationId));

    if (clientId != null) {
      final clientIdBytes = clientId!.codeUnits;
      byteBuffer.add(utils.int16(clientIdBytes.length));
      byteBuffer.add(clientIdBytes);
    } else {
      byteBuffer
          .add(utils.int16(-1)); // Null client_id is represented by length -1
    }

    return byteBuffer.toBytes();
  }

  /// Deserializes the ApiVersionResponse from a byte array.
  KafkaApiVersionResponse deserialize(Uint8List data) {
    if (data.length < 14) {
      print("Invalid byte array: Insufficient length for ApiVersionResponse");
    }

    final buffer = ByteData.sublistView(data);
    int offset = 0;

    // Read error_code (int16)
    final errorCode = buffer.getInt16(offset);
    offset += 2;

    // Read api_versions array length (int32)
    final apiVersionsLength = buffer.getInt32(offset);
    offset += 4;

    // Read each ApiVersion object
    final apiVersions = <ApiVersion>[];
    for (int i = 0; i < apiVersionsLength; i++) {
      final apiKey = buffer.getInt16(offset);
      offset += 2;

      final minVersion = buffer.getInt16(offset);
      offset += 2;

      final maxVersion = buffer.getInt16(offset);
      offset += 2;

      apiVersions.add(ApiVersion(
        apiKey: apiKey,
        minVersion: minVersion,
        maxVersion: maxVersion,
      ));
    }

    // Read throttle_time_ms (int32)
    final throttleTimeMs = buffer.getInt32(offset);
    offset += 4;

    return KafkaApiVersionResponse(
      errorCode: errorCode,
      apiVersions: apiVersions,
      throttleTimeMs: throttleTimeMs,
    );
  }
}
