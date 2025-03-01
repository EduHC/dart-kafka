import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/src/protocol/response_controller.dart';
import 'package:dart_kafka/src/typedefs/types.dart';

class KafkaClient {
  late final Socket? _socket;
  final String host;
  final int port;
  final ResponseController responseController = ResponseController();
  late final StreamSubscription? _subscription;

  Socket? get server => _socket;

  KafkaClient({required this.host, required this.port});

  Future<void> connect() async {
    _socket = await Socket.connect(host, port);
    if (_socket == null) throw Exception("Server hasn't connected");

    Future.microtask(() {
      _subscription = _socket.listen(
        (event) => _handleResponse(event),
      );
    });
  }

  Future<void> close() async {
    _socket?.close();
    _subscription?.cancel();
    _socket = null;
    _subscription = null;
  }

  void _handleResponse(Uint8List response) {
    responseController.enqueue(response);
    responseController.drainQueue();
  }

  void addPendingRequest(
      {required int correlationId, required Deserializer deserializer}) {
    responseController.addPendingRequest(
        correlationId: correlationId, deserializer: deserializer);
  }

  void completeRequest({required int correlationId}) {
    responseController.completeRequest(correlationId: correlationId);
  }

}
