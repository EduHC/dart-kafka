import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/definitions/errors.dart';
import 'package:dart_kafka/src/models/components/produce_response_component.dart';

class ErrorInterceptor {
  final KafkaAdmin admin;
  final KafkaClient kafka;

  ErrorInterceptor({required this.admin, required this.kafka});

  Future<({bool hasError, Map<String, dynamic>? errorInfo})> hasError(
      {required dynamic entity, required}) async {
    if (entity is FetchResponse) {
      for (Topic topic in entity.topics) {
        for (Partition partition in topic.partitions ?? []) {
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
      for (ProduceResponseComponent response in entity.responses) {
        for (Partition partition in response.partitions) {
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
