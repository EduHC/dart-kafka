import '../../dart_kafka.dart';
import '../definitions/errors.dart';
import '../models/components/produce_response_component.dart';

class ErrorInterceptor {
  ErrorInterceptor({required this.admin, required this.kafka});
  final KafkaAdmin admin;
  final KafkaClient kafka;

  Future<({bool hasError, Map<String, dynamic>? errorInfo})> hasError({
    required dynamic entity,
  }) async {
    if (entity is FetchResponse) {
      for (final Topic topic in entity.topics) {
        for (final Partition partition in topic.partitions ?? []) {
          if ((partition.errorCode ?? 0) == 0) continue;
          if ((partition.errorCode ?? 0) == 6) {
            await admin.updateTopicsMetadata(topics: kafka.topicsInUse);
          }
          return (
            hasError: true,
            errorInfo: ERROR_MAP[partition.errorCode ?? 0]
          );
        }
      }

      if (entity.errorCode == 0) return (hasError: false, errorInfo: null);
      return (hasError: true, errorInfo: ERROR_MAP[entity.errorCode]);
    } else if (entity is ProduceResponse) {
      for (final ProduceResponseComponent response in entity.responses) {
        for (final Partition partition in response.partitions) {
          if ((partition.errorCode ?? 0) == 0) continue;
          if ((partition.errorCode ?? 0) == 6) {
            await admin.updateTopicsMetadata(topics: kafka.topicsInUse);
          }
          return (hasError: true, errorInfo: ERROR_MAP[partition.errorCode]);
        }
      }

      return (hasError: false, errorInfo: null);
    }

    return (hasError: false, errorInfo: null);
  }
}
