import 'dart:typed_data';

import '../definitions/apis.dart';
import '../definitions/message_headers_version.dart';
import '../models/components/api_version.dart';
import '../models/responses/api_version_response.dart';
import '../protocol/decoder.dart';
import '../protocol/endocer.dart';
import '../protocol/utils.dart';

class KafkaVersionApi {
  final int apiKey = API_VERSIONS;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Serialize the ApiVersionRequest to Byte
  Uint8List serialize({
    required int correlationId,
    required int apiVersion,
    String? clientId,
    String clientSoftwareName = '',
    String clientSoftwareVersion = '',
  }) {
    final buffer = BytesBuilder();

    if (apiVersion > 2) {
      buffer
        ..add(encoder.compactString(clientSoftwareName))
        ..add(encoder.compactString(clientSoftwareVersion))
        ..add(encoder.uint8(0)); // _tagged_field
    }

    return Uint8List.fromList([
      ...encoder.writeMessageHeader(
        version: MessageHeaderVersion.requestHeaderVersion(
          apiVersion: apiVersion,
          apiKey: apiKey,
        ),
        messageLength: buffer.length,
        apiKey: apiKey,
        apiVersion: apiVersion,
        correlationId: correlationId,
        clientId: clientId,
      ),
      ...buffer.toBytes(),
    ]);
  }

  /// Deserializes the ApiVersionResponse from a byte array.
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    final errorCode = buffer.getInt16(offset);
    offset += 2;

    int arrayLength;
    if (apiVersion > 2) {
      final read = decoder.readCompactArrayLength(buffer, offset);
      arrayLength = read.value;
      offset += read.bytesRead;
    } else {
      arrayLength = buffer.getInt32(offset);
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

      int taggedField;
      if (apiVersion > 2) {
        taggedField = buffer.getInt8(offset);
        offset += 1;
      }

      apiVersions.add(
        ApiVersion(
          apiKey: apiKey,
          minVersion: minVersion,
          maxVersion: maxVersion,
        ),
      );
    }

    int? throttleTimeMs;
    if (apiVersion > 0) {
      buffer.getInt32(offset);
      offset += 4;
    }

    return ApiVersionResponse(
      version: apiVersion,
      errorCode: errorCode,
      apiVersions: apiVersions,
      throttleTimeMs: throttleTimeMs,
    );
  }
}
