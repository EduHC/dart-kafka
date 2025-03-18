import 'dart:collection';
import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

class RequestController {
  final Queue<_QueuedRequest> _requestQueue = Queue();
  final Socket server;

  RequestController({required this.server});

  Future<T> enqueue<T>(
      {required Uint8List request,
      bool async = true,
      required int correlationId}) {
    final completer = Completer<T>();

    _requestQueue.add(_QueuedRequest<T>(request, correlationId));

    if (_requestQueue.length == 1) {
      _processQueue();
    }

    return completer.future;
  }

  Future<void> _processQueue() async {
    while (_requestQueue.isNotEmpty) {
      var queuedRequest = _requestQueue.removeFirst();
      try {
        server.add(queuedRequest.request);
      } catch (e, stackTrace) {
        throw Exception(
            "Error while sending request ${queuedRequest.correlationId}! StackTrace: $stackTrace");
      }
    }
  }
}

class _QueuedRequest<T> {
  final Uint8List request;
  final int correlationId;

  _QueuedRequest(this.request, this.correlationId);
}
