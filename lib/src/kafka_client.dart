import 'dart:io';
import 'dart:typed_data';

class KafkaClient {
  late final Socket? _socket;
  final String host;
  final int port;

  Socket? get server => _socket;

  KafkaClient({required this.host, required this.port});

  Future<void> connect() async {
    _socket = await Socket.connect(host, port);
  }

  Future<void> close() async {
    _socket?.close();
  }

  Future<dynamic> sendRequestAndGetResponse(dynamic message, Function deserializer) async {
    if (_socket == null) {
      print("TCP connection not available! Try again later");
      return;
    }

    _socket.add(message);
    await _socket.flush();

    final responseBytes = await _socket.fold<Uint8List>(
      Uint8List(0),
      (previous, element) => Uint8List.fromList([...previous, ...element]),
    );

    return deserializer(responseBytes);
  }
}
