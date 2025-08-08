import '../api/fetch/fetch_response.dart';
import '../api/produce/produce_response.dart';
import '../client/kafka_admin.dart';
import '../client/kafka_client.dart';
import '../common/error.dart';
import '../common/partition.dart';
import '../common/produce_response_component.dart';
import '../common/topic.dart';

class ErrorMiddleware {
  ErrorMiddleware({required this.admin, required this.kafka});
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
