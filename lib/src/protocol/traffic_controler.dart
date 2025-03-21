import 'dart:async';
import 'dart:collection';
import 'package:collection/collection.dart';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/definitions/types.dart';
import 'package:dart_kafka/src/interceptors/error_interceptor.dart';
import 'package:dart_kafka/src/kafka_cluster.dart';

class TrafficControler {
  late final StreamController eventController;

  final Queue _messageQueue = Queue<List<int>>();
  final Queue<_Request> _pendingRequestQueue = Queue();

  final Map<int, _Request> _processingRequests = {};
  final Map<int, Map<String, dynamic>> _pendingResponses = {};
  final Map<int, Completer<dynamic>> _responseCompleters = {};
  final Queue<_RetryRequest> _retryRequestsQueue = Queue();

  final KafkaCluster cluster;

  TrafficControler({required this.cluster, required this.eventController});

  bool isDraining = false;
  bool get hasPendingProcesses =>
      _processingRequests.isNotEmpty ||
      _pendingResponses.isNotEmpty ||
      _pendingRequestQueue.isNotEmpty ||
      _messageQueue.isNotEmpty ||
      _responseCompleters.isNotEmpty ||
      _retryRequestsQueue.isNotEmpty;

  // Handle messages from Broker
  void enqueueBrokerMessage(Uint8List messages) {
    final byteData = ByteData.sublistView(messages);
    int offset = 0;

    while (offset < messages.length) {
      int messageLength = byteData.getInt32(offset);
      _messageQueue
          .add(messages.sublist(offset, (offset + messageLength + 4).toInt()));
      offset += messageLength + 4;
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
    print("Raw received: $response");
    final byteData = ByteData.sublistView(response);
    MessageHeader header = _extractMessageHeader(response);

    if (!_processingRequests.containsKey(header.correlationId)) {
      _pendingResponses.addAll({
        header.correlationId: {
          'apiVersion': header.apiVersion,
          'message': byteData.buffer.asUint8List().sublist(header.offset)
        }
      });
      return;
    }

    final int apiVersion =
        _processingRequests[header.correlationId]!.apiVersion;
    final deserializer = _processingRequests[header.correlationId]!.function;
    Uint8List message = byteData.buffer.asUint8List().sublist(header.offset);
    dynamic entity = deserializer(message, apiVersion);
    final entityAnalisys = _messageInterceptor(entity: entity);

    if (entityAnalisys.hasError && !entityAnalisys.errorInfo?['retry']) {
      throw Exception(entityAnalisys.errorInfo?['message']);
    }

    if (entityAnalisys.hasError) {
      _enqueueRetryRequest(_RetryRequest(
          correlationId: header.correlationId,
          req: _processingRequests[header.correlationId]!));
    }

    completeRequest(
        correlationId: header.correlationId,
        entity: entity,
        hasToRetry: entityAnalisys.hasError);
  }

  // Handle Messages from Application
  Future<dynamic> enqueuePendindRequest<T>(
      {bool async = true,
      required Uint8List message,
      required int correlationId,
      required int apiKey,
      required int apiVersion,
      required Deserializer function,
      String? topic,
      int? partition}) async {
    _Request? existing = _pendingRequestQueue.firstWhereOrNull(
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

    _pendingRequestQueue.add(_Request(
        apiKey: apiKey,
        apiVersion: apiVersion,
        function: function,
        message: message,
        partition: partition,
        topic: topic,
        async: async,
        correlationId: correlationId));

    if (_pendingRequestQueue.length == 1) {
      Future.microtask(() => _drainPendingRequestQueue());
    }

    if (async) return;

    return completer.future;
  }

  Future<void> _drainPendingRequestQueue() async {
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
    required _Request req,
  }) {
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

  void completeRequest(
      {required int correlationId,
      required dynamic entity,
      bool hasToRetry = false}) {
    if (!_processingRequests.containsKey(correlationId)) return;

    _processingRequests.removeWhere(
      (key, value) => key == correlationId,
    );

    if (hasToRetry) return;

    if (_responseCompleters.containsKey(correlationId)) {
      _responseCompleters[correlationId]!.complete(entity);
      _responseCompleters.remove(correlationId);
    } else {
      eventController.add(entity);
    }
  }

  MessageHeader _extractMessageHeader(Uint8List response) {
    final byteData = ByteData.sublistView(response);
    int offset = 0;

    final int messageLength = byteData.getInt32(offset, Endian.big);
    offset += 4;

    final int correlationId = byteData.getInt32(offset, Endian.big);
    offset += 4;

    return MessageHeader(
        messageLength: messageLength,
        correlationId: correlationId,
        offset: offset);
  }

  ({bool hasError, Map<String, dynamic>? errorInfo}) _messageInterceptor(
      {required dynamic entity}) {
    return ErrorInterceptor.hasError(entity: entity);
  }

  void _enqueueRetryRequest(_RetryRequest req) {
    _retryRequestsQueue.add(req);

    if (_retryRequestsQueue.length == 1) {
      _drainRetryRequests();
    }
  }

  Future<dynamic> _drainRetryRequests() async {
    while (_retryRequestsQueue.isNotEmpty) {
      var retryRequest = _retryRequestsQueue.removeFirst();
      sendRequestToBroker(req: retryRequest.req);
    }
  }

  Future<void> sendRequestToBroker({required _Request req}) async {
    Socket broker = API_REQUIRE_SPECIFIC_BROKER[req.apiKey]!
        ? cluster.getBrokerForPartition(
            topic: req.topic!, partition: req.partition!)
        : cluster.getAnyBroker();
    try {
      // print("**************************************");
      // print("Message sent: ${req.message}");
      // print("**************************************");
      broker.add(req.message);
    } catch (e, stackTrace) {
      throw Exception(
          "Error while sending request! ApiKey: ${req.apiKey} for version ${req.apiVersion}! StackTrace: $stackTrace");
    }
  }
}

// Helper
class _Request {
  final int apiKey;
  final int apiVersion;
  final Uint8List message;
  final Deserializer function;
  final String? topic;
  final int? partition;
  final int correlationId;
  final bool async;

  _Request(
      {required this.message,
      required this.apiKey,
      required this.apiVersion,
      required this.function,
      this.topic,
      this.partition,
      required this.async,
      required this.correlationId});
}

class _RetryRequest {
  final int correlationId;
  final _Request req;

  _RetryRequest({required this.correlationId, required this.req});
}
