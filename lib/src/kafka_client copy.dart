import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/kafka_cluster.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/protocol/request_controller.dart';
import 'package:dart_kafka/src/protocol/response_controller.dart';
import 'package:dart_kafka/src/protocol/utils.dart';
import 'package:dart_kafka/src/definitions/types.dart';

class KafkaClient {
  late final ResponseController responseController;
  late final RequestController requestController;
  late StreamSubscription? _subscription;
  final StreamController _eventController = StreamController();
  final KafkaCluster cluster = KafkaCluster();
  final Utils utils = Utils();
  final Encoder encoder = Encoder();

  Stream get eventStream => _eventController.stream.asBroadcastStream();

  bool started = false;
  bool consumerStarted = false;
  bool producerStarted = false;
  bool adminStarted = false;

  /// @Parameter brokers
  ///    A list containing only the Host Address and the Port to access N Kafka Brokers
  KafkaClient({required List<Broker> brokers}) {
    cluster.setBrokers(brokers);
    responseController = ResponseController(eventController: _eventController);
  }

  Future<void> connect() async {
    if (started) return;
    started = true;
    cluster.connect(responseHanddler: _handleResponse);
  }

  Future<void> close() async {
    while (hasPendingProcesses()) {
      await Future.delayed(Duration(seconds: 1));
      continue;
    }
    print("Fechou");
    cluster.close();
    _subscription?.cancel();
    _subscription = null;
  }

  void _handleResponse(Uint8List response) {
    responseController.enqueue(response);
    responseController.drainQueue();
  }

  Future<dynamic> storeProcessingRequest(
      {required int correlationId,
      required Deserializer deserializer,
      required int apiVersion,
      bool async = true}) async {
    Future<dynamic> res = responseController.storeProcessingRequest(
        correlationId: correlationId,
        deserializer: deserializer,
        apiVersion: apiVersion,
        async: async);

    if (async) return;

    return res;
  }

  bool hasPendingProcesses() {
    return responseController.hasPendingProcesses;
  }

  Future<void> enqueueRequest(
      {required Uint8List request,
      required int correlationId,
      required bool async}) async {
    return requestController.enqueue(
        request: request, correlationId: correlationId, async: async);
  }
}
