import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/src/protocol/response_controller.dart';
import 'package:dart_kafka/src/typedefs/types.dart';

class KafkaClient {
  late Socket? _socket;
  final String host;
  final int port;
  final ResponseController responseController = ResponseController();
  late StreamSubscription? _subscription;

  Socket? get server => _socket;

  KafkaClient({required this.host, required this.port});

  Future<void> connect() async {
    _socket = await Socket.connect(host, port);
    if (_socket == null) throw Exception("Server hasn't connected");

    _socket!.setOption(SocketOption.tcpNoDelay, true);
    Future.microtask(() {
      _subscription = _socket!.listen(
        (event) => _handleResponse(event),
      );
    });
  }

  Future<void> close() async {
    while (hasPendingProcesses()) {
      await Future.delayed(Duration(seconds: 1));
      continue;
    }
    print("Fechou");
    _subscription?.cancel();
    _socket?.close();
    _socket?.destroy();
    _subscription = null;
    _socket = null;
  }

  void _handleResponse(Uint8List response) {
    responseController.enqueue(response);
    responseController.drainQueue();
  }

  void addPendingRequest(
      {required int correlationId,
      required Deserializer deserializer,
      required int apiVersion}) {
    responseController.addPendingRequest(
        correlationId: correlationId,
        deserializer: deserializer,
        apiVersion: apiVersion);
  }

  void completeRequest({required int correlationId}) {
    responseController.completeRequest(correlationId: correlationId);
  }

  bool hasPendingProcesses() {
    return responseController.hasPendingProcesses;
  }
}
