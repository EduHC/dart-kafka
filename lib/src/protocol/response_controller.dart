import 'dart:async';
import 'dart:collection';
import 'dart:typed_data';

import 'package:dart_kafka/src/interceptors/error_interceptor.dart';
import 'package:dart_kafka/src/models/components/message_header.dart';
import 'package:dart_kafka/src/definitions/types.dart';

class ResponseController {
  final HashMap<int, Map<String, dynamic>> _pendingRequests = HashMap();
  final HashMap<int, Map<String, dynamic>> _pendingResponses = HashMap();
  final Queue _messageQueue = Queue<List<int>>();
  final HashMap<int, Completer<dynamic>> _responseCompleters = HashMap();
  late final StreamController eventController;

  ResponseController({required this.eventController});

  bool isDraining = false;

  bool get hasPendingProcesses =>
      _pendingRequests.isNotEmpty || _pendingResponses.isNotEmpty;

  Future<dynamic> waitForResponse(int correlationId) {
    final completer = Completer<dynamic>();
    _responseCompleters[correlationId] = completer;
    return completer.future;
  }

  void enqueue(Uint8List messages) {
    final byteData = ByteData.sublistView(messages);
    int offset = 0;

    while (offset < messages.length) {
      int messageLength = byteData.getInt32(offset);
      _messageQueue
          .add(messages.sublist(offset, (offset + messageLength + 4).toInt()));
      offset += messageLength + 4;
    }
  }

  Future<void> drainQueue() async {
    if (isDraining) {
      return;
    }

    while (_messageQueue.isNotEmpty) {
      isDraining = true;
      final element = _messageQueue.removeFirst();
      handleResponse(element);
    }

    isDraining = false;
  }

  Future<void> handleResponse(Uint8List response) async {
    // print("Raw received: $response");
    final byteData = ByteData.sublistView(response);
    MessageHeader header = _extractMessageHeader(response);

    if (!_pendingRequests.containsKey(header.correlationId)) {
      _pendingResponses.addAll({
        header.correlationId: {
          'apiVersion': header.apiVersion,
          'message': byteData.buffer.asUint8List().sublist(header.offset)
        }
      });
      return;
    }

    final int apiVersion = _pendingRequests[header.correlationId]!['apiVersion'];
    final deserializer = _pendingRequests[header.correlationId]!['function'];
    Uint8List message = byteData.buffer.asUint8List().sublist(header.offset);
    dynamic entity = deserializer(message, apiVersion);
    final entityAnalisys = _messageInterceptor(entity: entity);

    if (entityAnalisys.hasError && entityAnalisys.errorInfo?['retry']) {
      // TODO: allocate the request to Retry
      return;
    }

    if (entityAnalisys.hasError) {
      throw Exception(entityAnalisys.errorInfo?['message']);
    }

    completeRequest(correlationId: header.correlationId, entity: entity);
  }

  Future<dynamic> storeProcessingRequest({
    required int correlationId,
    required Deserializer deserializer,
    required int apiVersion,
    required bool async,
  }) async {
    if (_pendingRequests.containsKey(correlationId)) return;

    if (_pendingResponses.containsKey(correlationId)) {
      deserializer(
        _pendingResponses[correlationId]!['message'],
        _pendingResponses[correlationId]!['apiVersion'],
      );
      return;
    }

    _pendingRequests[correlationId] = {
      'apiVersion': apiVersion,
      'function': deserializer,
    };

    if (async) return;

    return waitForResponse(correlationId);
  }

  void completeRequest({required int correlationId, required dynamic entity}) {
    if (!_pendingRequests.containsKey(correlationId)) return;

    _pendingRequests.removeWhere(
      (key, value) => key == correlationId,
    );

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
}
