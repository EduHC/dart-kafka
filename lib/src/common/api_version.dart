class ApiVersion {
  ApiVersion({
    required this.apiKey,
    required this.minVersion,
    required this.maxVersion,
  });
  final int apiKey;
  final int minVersion;
  final int maxVersion;

  @override
  String toString() =>
      'ApiVersion(apiKey: $apiKey, minVersion: $minVersion, maxVersion: $maxVersion)';
}
