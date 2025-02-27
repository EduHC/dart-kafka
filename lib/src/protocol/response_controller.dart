import 'dart:collection';
import 'dart:typed_data';

import 'package:dart_kafka/src/models/message_header.dart';

typedef Deserializer = dynamic Function(Uint8List data);

class ResponseController {
  final HashMap<int, Function> _pendingRequests = HashMap<int, Function>();
  final HashMap<int, Uint8List> _pendingResponses = HashMap<int, Uint8List>();

  Future<void> handleResponse(Uint8List response) async {
    if (response.length < 14) {
      print("Invalid byte array");
      return;
    }

    final byteData = ByteData.sublistView(response);

    MessageHeader header = _extractMessageHeader(response);

    if (!_pendingRequests.containsKey(header.correlationId)) {
      _pendingResponses.addAll({
        header.correlationId:
            byteData.buffer.asUint8List().sublist(header.offset)
      });
      return;
    }
    
    final deserializer = _pendingRequests[header.correlationId]!;
    deserializer(byteData.buffer.asUint8List().sublist(header.offset));
    completeRequest(correlationId: header.correlationId);
  }

  void addPendingRequest(
      {required int correlationId, required Deserializer deserializer}) {
    if (_pendingRequests.containsKey(correlationId)) return;

    if (_pendingResponses.containsKey(correlationId)) {
      deserializer(_pendingResponses[correlationId]!);
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
