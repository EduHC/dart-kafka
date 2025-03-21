import 'dart:typed_data';

import 'package:dart_kafka/src/models/components/api_version.dart';

class ApiVersionResponse {
  final int version;
  final int errorCode;
  final List<ApiVersion> apiVersions;
  final int? throttleTimeMs;

  ApiVersionResponse({
    required this.version,
    required this.errorCode,
    required this.apiVersions,
    required this.throttleTimeMs,
  });

  @override
  String toString() {
    return 'KafkaApiVersionResponse(version: $version, errorCode: $errorCode, apiVersions: $apiVersions, throttleTimeMs: $throttleTimeMs)';
  }
}
