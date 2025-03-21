import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/models/components/produce_response_component.dart';

class ErrorInterceptor {
  static ({bool hasError, Map<String, dynamic>? errorInfo}) hasError(
      {required dynamic entity}) {
    if (entity is FetchResponse) {
      if (entity.errorCode == 0) return (hasError: false, errorInfo: null);
    } else if (entity is ProduceResponse) {
      for (ProduceResponseComponent response in entity.responses) {
        for (Partition partition in response.partitions) {
          if ((partition.errorCode ?? 0) == 0) continue;
        }
      }

      return (hasError: false, errorInfo: null);
    }

    return (hasError: false, errorInfo: null);
  }
}
