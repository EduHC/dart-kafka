import 'dart:typed_data';

import 'package:dart_kafka/src/models/api_version.dart';

class KafkaApiVersionResponse {
  final int version;
  final int errorCode;
  final List<ApiVersion> apiVersions;
  final int? throttleTimeMs;

  KafkaApiVersionResponse({
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
