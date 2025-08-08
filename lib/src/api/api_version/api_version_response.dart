import '../../common/api_version.dart';

class ApiVersionResponse {
  ApiVersionResponse({
    required this.version,
    required this.errorCode,
    required this.apiVersions,
    required this.throttleTimeMs,
  });
  final int version;
  final int errorCode;
  final List<ApiVersion> apiVersions;
  final int? throttleTimeMs;

  @override
  String toString() =>
      'KafkaApiVersionResponse(version: $version, errorCode: $errorCode, apiVersions: $apiVersions, throttleTimeMs: $throttleTimeMs)';
}
