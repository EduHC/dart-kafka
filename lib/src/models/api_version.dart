class ApiVersion {
  final int apiKey;
  final int minVersion;
  final int maxVersion;

  ApiVersion({
    required this.apiKey,
    required this.minVersion,
    required this.maxVersion,
  });

  @override
  String toString() {
    return 'ApiVersion(apiKey: $apiKey, minVersion: $minVersion, maxVersion: $maxVersion)';
  }
}
