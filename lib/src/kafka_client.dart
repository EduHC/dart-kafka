import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/kafka_cluster.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/protocol/request_controller.dart';
import 'package:dart_kafka/src/protocol/response_controller.dart';
import 'package:dart_kafka/src/protocol/traffic_controler.dart';
import 'package:dart_kafka/src/protocol/utils.dart';
import 'package:dart_kafka/src/definitions/types.dart';

class KafkaClient {
  late final ResponseController _responseController;
  late final RequestController _requestController = RequestController();

  late final TrafficControler _trafficControler;

  final StreamController _eventController = StreamController();
  final KafkaCluster _cluster = KafkaCluster();
  final List<Broker> brokers = [];
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
    _cluster.setBrokers(brokers);
    _trafficControler =
        TrafficControler(cluster: _cluster, eventController: _eventController);
    _responseController = ResponseController(eventController: _eventController);
  }

  Future<void> connect() async {
    if (started) return;
    await _cluster.connect(responseHandler: _handleResponse);
    started = true;
  }

  Future<void> close() async {
    print("Acionada função para fechar.");
    while (hasPendingProcesses()) {
      await Future.delayed(Duration(seconds: 1));
      continue;
    }
    _cluster.close();
    print("Fechou.");
  }

  // Request/Response controllers
  void _handleResponse(Uint8List response) {
    _trafficControler.enqueueBrokerMessage(response);
    _trafficControler.drainBrokerMessagesQueue();
    // _responseController.enqueue(response);
    // _responseController.drainQueue();
  }

  Future<dynamic> storeProcessingRequest(
      {required int correlationId,
      required Deserializer deserializer,
      required int apiVersion,
      bool async = true}) async {
    Future<dynamic> res = _responseController.storeProcessingRequest(
        correlationId: correlationId,
        deserializer: deserializer,
        apiVersion: apiVersion,
        async: async);

    if (async) return;

    return res;
  }

  bool hasPendingProcesses() {
    return _responseController.hasPendingProcesses &&
        _requestController.hasPendingProcesses;
  }

  Future<void> enqueueRequest(
      {required Uint8List request,
      required int correlationId,
      required bool async,
      required Socket sock}) async {
    return _requestController.enqueue(
        request: request,
        correlationId: correlationId,
        async: async,
        sock: sock);
  }

  // Broker Related functions
  Socket getBrokerForPartition(
      {required String topic, required int partition}) {
    return _cluster.getBrokerForPartition(topic: topic, partition: partition);
  }

  void updateTopicsBroker({required MetadataResponse metadata}) {
    _cluster.updateTopicsBroker(metadata: metadata);
  }

  Socket getAnyBroker() {
    return _cluster.getAnyBroker();
  }

  Future<dynamic> tEnqueueRequest(
      {required Uint8List message,
      required int correlationId,
      required int apiKey,
      required int apiVersion,
      required Deserializer function,
      String? topic,
      int? partition,
      bool async = true}) async {
    Future<dynamic> res = _trafficControler.enqueuePendindRequest(
        message: message,
        correlationId: correlationId,
        apiKey: apiKey,
        apiVersion: apiVersion,
        function: function,
        topic: topic,
        partition: partition,
        async: async);

    if (async) return;
    
    return res;
  }
}
