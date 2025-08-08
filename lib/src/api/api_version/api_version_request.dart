import 'dart:convert';

class ApiVersionRequest {
  final int correlationId;
  final int apiVersion;
  final String? clientId;
  final String clientSoftwareName;
  final String clientSoftwareVersion;

  ApiVersionRequest({
    required this.correlationId,
    required this.apiVersion,
    required this.clientSoftwareName,
    required this.clientSoftwareVersion,
    this.clientId,
  });

  ApiVersionRequest copyWith({
    int? correlationId,
    int? apiVersion,
    String? clientId,
    String? clientSoftwareName,
    String? clientSoftwareVersion,
  }) =>
      ApiVersionRequest(
        correlationId: correlationId ?? this.correlationId,
        apiVersion: apiVersion ?? this.apiVersion,
        clientId: clientId ?? this.clientId,
        clientSoftwareName: clientSoftwareName ?? this.clientSoftwareName,
        clientSoftwareVersion:
            clientSoftwareVersion ?? this.clientSoftwareVersion,
      );

  Map<String, dynamic> toMap() => <String, dynamic>{
        'correlationId': correlationId,
        'apiVersion': apiVersion,
        'clientId': clientId,
        'clientSoftwareName': clientSoftwareName,
        'clientSoftwareVersion': clientSoftwareVersion,
      };

  factory ApiVersionRequest.fromMap(Map<String, dynamic> map) =>
      ApiVersionRequest(
        correlationId: map['correlationId'] as int,
        apiVersion: map['apiVersion'] as int,
        clientId: map['clientId'] != null ? map['clientId'] as String : null,
        clientSoftwareName: map['clientSoftwareName'] as String,
        clientSoftwareVersion: map['clientSoftwareVersion'] as String,
      );

  String toJson() => json.encode(toMap());

  factory ApiVersionRequest.fromJson(String source) =>
      ApiVersionRequest.fromMap(json.decode(source) as Map<String, dynamic>);

  @override
  String toString() =>
      'ApiVersionRequest(correlationId: $correlationId, apiVersion: $apiVersion, clientId: $clientId, clientSoftwareName: $clientSoftwareName, clientSoftwareVersion: $clientSoftwareVersion)';

  @override
  bool operator ==(covariant ApiVersionRequest other) {
    if (identical(this, other)) return true;

    return other.correlationId == correlationId &&
        other.apiVersion == apiVersion &&
        other.clientId == clientId &&
        other.clientSoftwareName == clientSoftwareName &&
        other.clientSoftwareVersion == clientSoftwareVersion;
  }

  @override
  int get hashCode =>
      correlationId.hashCode ^
      apiVersion.hashCode ^
      clientId.hashCode ^
      clientSoftwareName.hashCode ^
      clientSoftwareVersion.hashCode;
}
