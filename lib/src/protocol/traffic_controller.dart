import 'dart:async';
import 'dart:collection';
import 'package:collection/collection.dart';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/definitions/message_headers_version.dart';
import 'package:dart_kafka/src/definitions/types.dart';
import 'package:dart_kafka/src/interceptors/error_interceptor.dart';
import 'package:dart_kafka/src/kafka_cluster.dart';
import 'package:dart_kafka/src/models/request.dart';

class TrafficControler {
  late final StreamController eventController;

  final Queue _messageQueue = Queue<List<int>>();
  final Queue<Request> _pendingRequestQueue = Queue();

  final Map<int, Request> _processingRequests = {};
  final Map<int, Map<String, dynamic>> _pendingResponses = {};
  final Map<int, Completer<dynamic>> _responseCompleters = {};
  final Queue<_RetryRequest> _retryRequestsQueue = Queue();
  final Queue<Request> _requestsToCommit = Queue();

  final KafkaCluster cluster;
  final KafkaAdmin admin;
  final KafkaClient kafka;

  late final ErrorInterceptor errorInterceptor;

  TrafficControler({
    required this.cluster,
    required this.eventController,
    required this.admin,
    required this.kafka,
  }) {
    errorInterceptor = ErrorInterceptor(kafka: kafka, admin: admin);
  }

  bool get hasPendingProcesses =>
      _processingRequests.isNotEmpty ||
      _pendingResponses.isNotEmpty ||
      _pendingRequestQueue.isNotEmpty ||
      _messageQueue.isNotEmpty ||
      _responseCompleters.isNotEmpty ||
      _retryRequestsQueue.isNotEmpty;

  bool isDraining = false;
  final List<int> _buffer = [];

  // Handle messages from Broker
  void enqueueBrokerMessage(Uint8List messages) {
    _buffer.addAll(messages);

    while (_buffer.length >= 4) {
      final byteData = ByteData.sublistView(Uint8List.fromList(_buffer));
      int messageLength = byteData.getInt32(0, Endian.big);

      if (_buffer.length < messageLength + 4 || messageLength < 0) {
        // Wait for more data if the complete message is not available
        return;
      }

      // Extract complete message
      final completeMessage = Uint8List.fromList(
        _buffer.sublist(0, messageLength + 4),
      );
      _buffer.removeRange(0, messageLength + 4);

      _messageQueue.add(completeMessage);
    }
  }

  Future<void> drainBrokerMessagesQueue() async {
    if (isDraining) {
      return;
    }

    while (_messageQueue.isNotEmpty) {
      isDraining = true;
      final element = _messageQueue.removeFirst();
      handleBrokerMessageResponse(element);
    }

    isDraining = false;
  }

  Future<void> handleBrokerMessageResponse(Uint8List response) async {
    // print("Raw received: $response");
    final byteData = ByteData.sublistView(response);

    int offset = 0;
    final int messageLength = byteData.getInt32(offset, Endian.big);
    offset += 4;

    if (messageLength < 0 || messageLength > response.length) {
      print("[KAFKA-TRAFFIC-CONTROLLER] ERROR DECODING MESSAGE LENGTH! $messageLength of ${response.length}");
      return;
    }

    final int correlationId = byteData.getInt32(offset, Endian.big);
    offset += 4;

    Request? req = _processingRequests[correlationId];

    if (req == null) return;

    MessageHeader header = _extractMessageHeader(
      response: response,
      apiKey: req.apiKey,
      apiVersion: req.apiVersion,
    );

    final int apiVersion = _processingRequests[correlationId]!.apiVersion;
    final deserializer = _processingRequests[correlationId]!.function;
    Uint8List message = byteData.buffer.asUint8List().sublist(header.offset);
    dynamic entity = deserializer(message, apiVersion);
    // print("Decoded Entity: $entity");
    final entityAnalisys = await _messageInterceptor(entity: entity);

    if (entityAnalisys.hasError && !entityAnalisys.errorInfo?['retry']) {
      throw Exception(entityAnalisys.errorInfo?['message']);
    }

    if (entityAnalisys.hasError) {
      _enqueueRetryRequest(
        _RetryRequest(
          correlationId: correlationId,
          req: _processingRequests[correlationId]!,
        ),
      );
      return;
    }

    completeRequest(
      correlationId: correlationId,
      entity: entity,
      hasToRetry: entityAnalisys.hasError,
    );
  }

  // Handle Messages from Application
  Future<dynamic> enqueuePendindRequest<T>({
    bool async = true,
    required Uint8List message,
    required int correlationId,
    required int apiKey,
    required int apiVersion,
    required Deserializer function,
    required bool autoCommit,
    String? topicName,
    int? partition,
    Socket? broker,
    String? groupId,
    String? memberId,
    String? groupInstanceId,
    Topic? topic,
  }) async {
    // print(
    //     "${DateTime.now()} || [KAFKA-TRAFFIC-CONTROLLER] Entrou para alocar a request na fila. Topic: $topicName");
    Request? existing = _pendingRequestQueue.firstWhereOrNull(
      (req) => req.correlationId == correlationId,
    );

    if (existing != null) return;

    if (_pendingResponses.containsKey(correlationId)) {
      print("Encontrou a resposta antes da request -- pending request");
      function(
        _pendingResponses[correlationId]!['message'],
        _pendingResponses[correlationId]!['apiVersion'],
      );
      return;
    }

    final completer = Completer<T>();
    _responseCompleters[correlationId] = completer;

    _pendingRequestQueue.add(
      Request(
        apiKey: apiKey,
        apiVersion: apiVersion,
        function: function,
        message: message,
        partition: partition,
        topicName: topicName,
        async: async,
        correlationId: correlationId,
        broker: broker,
        topic: topic,
        groupId: groupId,
        memberId: memberId,
        groupInstanceId: groupInstanceId,
        autoCommit: autoCommit,
      ),
    );

    if (_pendingRequestQueue.length == 1) {
      Future.microtask(() => _drainPendingRequestQueue());
    }

    if (async) {
      _responseCompleters.removeWhere(
        (key, value) => key == correlationId,
      );
      return;
    }

    return completer.future;
  }

  Future<void> _drainPendingRequestQueue() async {
    // print(
    //     "${DateTime.now()} || [KAFKA-TRAFFIC-CONTROLLER] Entrou para drenar as requests pendentes. Total na fila ${_pendingRequestQueue.length}");
    while (_pendingRequestQueue.isNotEmpty) {
      var pendingRequest = _pendingRequestQueue.removeFirst();
      _enqueueProcessingRequest(
        req: pendingRequest,
        correlationId: pendingRequest.correlationId,
      );
      sendRequestToBroker(req: pendingRequest);
    }
  }

  void _enqueueProcessingRequest({
    required int correlationId,
    required Request req,
  }) {
    // print(
    //     "${DateTime.now()} || [KAFKA-TRAFFIC-CONTROLLER] Alocando request como Processing. Topic: ${req.topicName}");
    if (_processingRequests.containsKey(correlationId)) return;

    if (_pendingResponses.containsKey(correlationId)) {
      print("Encontrou a resposta antes da request");
      req.function(
        _pendingResponses[correlationId]!['message'],
        _pendingResponses[correlationId]!['apiVersion'],
      );
      return;
    }

    _processingRequests[correlationId] = req;
  }

  void completeRequest({
    required int correlationId,
    required dynamic entity,
    bool hasToRetry = false,
  }) {
    // print(
    //     "${DateTime.now()} || [KAFKA-TRAFFIC-CONTROLLER] Completando request com resposta do Broker. Topic ${_processingRequests[correlationId]!.topicName}");
    if (!_processingRequests.containsKey(correlationId)) return;
    if (hasToRetry) return;

    final Request req = _processingRequests[correlationId]!;

    if (req.autoCommit &&
        req is FetchResponse &&
        _validadeFetchRequestResult(res: entity)) {
      kafka.getConsumerClient().updateMemberOffsetFromLocal(
            groupId: req.groupId!,
            memberId: req.memberId!,
            topics: (entity as FetchResponse).topics,
          );

      _requestsToCommit.add(req);

      if (_requestsToCommit.length == 1) {
        _drainRequestsToCommit();
      }
    }

    _processingRequests.removeWhere(
      (key, value) => key == correlationId,
    );

    if (_responseCompleters.containsKey(correlationId)) {
      _responseCompleters[correlationId]!.complete(entity);
      _responseCompleters.remove(correlationId);
    } else {
      eventController.add(entity);
    }
  }

  MessageHeader _extractMessageHeader({
    required Uint8List response,
    required int apiKey,
    required int apiVersion,
  }) {
    final byteData = ByteData.sublistView(response);
    int offset = 8;

    int headerVersion = MessageHeaderVersion.responseHeaderVersion(
      apiKey: apiKey,
      apiVersion: apiVersion,
    );

    if (headerVersion > 0) {
      final int taggedField = byteData.getInt8(offset);
      offset += 1;
    }

    return MessageHeader(offset: offset);
  }

  Future<({bool hasError, Map<String, dynamic>? errorInfo})>
      _messageInterceptor({required dynamic entity}) async {
    return await errorInterceptor.hasError(entity: entity);
  }

  void _enqueueRetryRequest(_RetryRequest req) {
    _retryRequestsQueue.add(req);
    print(
        "Setando requisição p/ Retry: ${req.correlationId} | ApiKey: ${req.req.apiKey} - version: ${req.req.apiVersion}");

    if (_retryRequestsQueue.length == 1) {
      Future.microtask(() => _drainRetryRequests());
    }
  }

  Future<dynamic> _drainRetryRequests() async {
    print("Entrou p/ drenar a fila de Retry");
    while (_retryRequestsQueue.isNotEmpty) {
      var retryRequest = _retryRequestsQueue.removeFirst();
      _enqueueProcessingRequest(
        correlationId: retryRequest.correlationId,
        req: retryRequest.req,
      );
      sendRequestToBroker(req: retryRequest.req);
    }
  }

  Future<void> sendRequestToBroker({required Request req}) async {
    // print(
    //     "${DateTime.now()} || [KAFKA-TRAFFIC-CONTROLLER] Enviando request para o Broker. Topic: ${req.topicName}");
    Socket broker = req.broker ??
        (API_REQUIRE_SPECIFIC_BROKER[req.apiKey]!
            ? cluster.getBrokerForPartition(
                topic: req.topicName!, partition: req.partition!)
            : cluster.getAnyBroker());
    try {
      // print("**************************************");
      // print("Broker: ${broker.address.host}:${broker.remotePort}");
      // print("Message sent: ${req.message}");
      // print("**************************************");
      broker.add(req.message);
    } catch (e, stackTrace) {
      throw Exception(
          "Error while sending request! ApiKey: ${req.apiKey} for version ${req.apiVersion}! StackTrace: $stackTrace");
    }
  }

  Future<void> _drainRequestsToCommit() async {
    while (_requestsToCommit.isNotEmpty) {
      final Request requestToCommit = _requestsToCommit.removeFirst();
      kafka.commitMessage(req: requestToCommit);
    }
  }

  bool _validadeFetchRequestResult({required FetchResponse res}) {
    Topic topic = res.topics.first;

    if (topic.partitions == null) {
      throw Exception("Retrived a FetchRespose without Partitions");
    }

    Partition part = topic.partitions!.first;
    if (part.batch == null) return false;
    if (part.batch!.records == null) return false;

    return true;
  }
}

// Helper
class _RetryRequest {
  final int correlationId;
  final Request req;

  _RetryRequest({
    required this.correlationId,
    required this.req,
  });
}
