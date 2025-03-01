import 'dart:collection';
import 'dart:typed_data';

import 'package:dart_kafka/src/models/components/message_header.dart';
import 'package:dart_kafka/src/typedefs/types.dart';

class ResponseController {
  final HashMap<int, Function> _pendingRequests = HashMap();
  final HashMap<int, Map<String, dynamic>> _pendingResponses = HashMap();
  final Queue _messageQueue = Queue<List<int>>();
  bool isDraining = false;

  void enqueue(Uint8List messages) {
    final byteData = ByteData.sublistView(messages);
    int offset = 0;

    while (offset < messages.length) {
      int messageLength = byteData.getInt32(offset);
      _messageQueue.add(messages.sublist(offset, (offset + messageLength + 4).toInt()));
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
    if (response.length < 14) {
      print("Invalid byte array");
      return;
    }

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

    final deserializer = _pendingRequests[header.correlationId]!;
    Uint8List message = byteData.buffer.asUint8List().sublist(header.offset);
    dynamic entity = deserializer(message, header.apiVersion);
    print("Event = $entity");
    completeRequest(correlationId: header.correlationId);
  }

  void addPendingRequest(
      {required int correlationId, required Deserializer deserializer}) {
    if (_pendingRequests.containsKey(correlationId)) return;

    if (_pendingResponses.containsKey(correlationId)) {
      deserializer(_pendingResponses[correlationId]!['message'],
          _pendingResponses[correlationId]!['apiVersion']);
      return;
    }

    _pendingRequests.addAll({correlationId: deserializer});
  }

  void completeRequest({required int correlationId}) {
    if (!_pendingRequests.containsKey(correlationId)) return;
    _pendingRequests.removeWhere(
      (key, value) => key == correlationId,
    );
  }

  MessageHeader _extractMessageHeader(Uint8List response) {
    final byteData = ByteData.sublistView(response);
    int offset = 0;

    final int messageLength = byteData.getInt32(offset);
    offset += 4;

    final int correlationId = byteData.getInt32(offset);
    offset += 4;

    final int apiKey = byteData.getInt16(offset);
    offset += 2;

    final int apiVersion = byteData.getInt16(offset);
    offset += 2;

    return MessageHeader(
        apiKey: apiKey,
        apiVersion: apiVersion,
        messageLength: messageLength,
        correlationId: correlationId,
        offset: offset);
  }
}
