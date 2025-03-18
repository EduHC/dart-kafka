import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/protocol/request_controller.dart';
import 'package:dart_kafka/src/protocol/response_controller.dart';
import 'package:dart_kafka/src/protocol/utils.dart';
import 'package:dart_kafka/src/definitions/types.dart';

class KafkaClient {
  late Socket? _socket;
  final String host;
  final int port;
  late final ResponseController responseController;
  late final RequestController requestController;
  late StreamSubscription? _subscription;
  final StreamController _eventController = StreamController();

  final Utils utils = Utils();
  final Encoder encoder = Encoder();

  Stream get eventStream => _eventController.stream.asBroadcastStream();
  Socket? get server => _socket;

  KafkaClient({required this.host, required this.port}) {
    responseController = ResponseController(eventController: _eventController);
  }

  Future<void> connect() async {
    _socket = await Socket.connect(host, port);
    if (_socket == null) throw Exception("Server hasn't connected");

    _socket!.setOption(SocketOption.tcpNoDelay, true);
    requestController = RequestController(server: _socket!);
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
