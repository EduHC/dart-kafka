import 'dart:typed_data';

import 'package:dart_kafka/src/models/api_version.dart';

class KafkaApiVersionResponse {
  final int errorCode;
  final List<ApiVersion> apiVersions;
  final int throttleTimeMs;

  KafkaApiVersionResponse({
    required this.errorCode,
    required this.apiVersions,
    required this.throttleTimeMs,
  });

  @override
  String toString() {
    return 'KafkaApiVersionResponse(errorCode: $errorCode, apiVersions: $apiVersions, throttleTimeMs: $throttleTimeMs)';
  }
}
