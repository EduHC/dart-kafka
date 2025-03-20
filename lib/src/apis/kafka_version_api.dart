import 'dart:typed_data';

import 'package:dart_kafka/src/definitions/message_headers_version.dart';
import 'package:dart_kafka/src/models/components/api_version.dart';
import 'package:dart_kafka/src/models/responses/api_version_response.dart';
import 'package:dart_kafka/src/protocol/decoder.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/protocol/utils.dart';
import 'package:dart_kafka/src/definitions/apis.dart';

class KafkaVersionApi {
  final int apiKey = API_VERSIONS;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Serialize the ApiVersionRequest to Byte
  Uint8List serialize({
    required int correlationId,
    String? clientId,
    required int apiVersion,
    String clientSoftwareName = '',
    String clientSoftwareVersion = '',
  }) {
    final buffer = BytesBuilder();

    if (apiVersion > 2) {
      buffer.add(encoder.compactString(clientSoftwareName));
      buffer.add(encoder.compactString(clientSoftwareVersion));
    }

    return Uint8List.fromList([
      ...encoder.writeMessageHeader(
          version: MessageHeaderVersion.requestHeaderVersion(
              apiVersion: apiVersion, apiKey: apiKey),
          messageLength: buffer.length,
          apiKey: apiKey,
          apiVersion: apiVersion,
          correlationId: correlationId,
          clientId: clientId),
      ...buffer.toBytes()
    ]);
  }

  /// Deserializes the ApiVersionResponse from a byte array.
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    final errorCode = buffer.getInt16(offset, Endian.big);
    offset += 2;

    int arrayLength;
    if (apiVersion > 2) {
      final read = decoder.readCompactArrayLength(buffer, offset);
      arrayLength = read.value;
      offset += read.bytesRead;
    } else {
      arrayLength = buffer.getInt32(offset, Endian.big);
      offset += 4;
    }

    final apiVersions = <ApiVersion>[];
    for (int i = 0; i < arrayLength; i++) {
      final apiKey = buffer.getInt16(offset);
      offset += 2;

      final minVersion = buffer.getInt16(offset);
      offset += 2;

      final maxVersion = buffer.getInt16(offset);
      offset += 2;

      int _tagged_field;
      if (apiVersion > 2) {
        _tagged_field = buffer.getInt8(offset);
        offset += 1;
      }

      apiVersions.add(ApiVersion(
        apiKey: apiKey,
        minVersion: minVersion,
        maxVersion: maxVersion,
      ));
    }

    int? throttleTimeMs;
    if (apiVersion > 0) {
      buffer.getInt32(offset);
      offset += 4;
    }

    return KafkaApiVersionResponse(
      version: apiVersion,
      errorCode: errorCode,
      apiVersions: apiVersions,
      throttleTimeMs: throttleTimeMs,
    );
  }
}
