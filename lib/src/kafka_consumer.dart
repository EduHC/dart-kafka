import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/src/kafka_joing_group.dart';

class KafkaConsumer {
  final String _bootstrapServers;
  final String _groupId;
  final String _topic;
  final Map<String, String> _config;
  final int _protocolVersion;
  final int sessionTimeoutMs;
  final int rebalanceTimeoutMs;

  Socket? _socket;
  bool _isRunning = false;
  String _memberId = ''; // Member ID assigned by the broker

  KafkaConsumer({
    required String bootstrapServers,
    required String groupId,
    required String topic,
    required int protocolVersion,
    this.sessionTimeoutMs = 10000,
    this.rebalanceTimeoutMs = 30000,
    Map<String, String>? config,
  })  : _bootstrapServers = bootstrapServers,
        _groupId = groupId,
        _topic = topic,
        _protocolVersion = protocolVersion,
        _config = config ?? {};

  Future<void> connect() async {
    final server = _bootstrapServers.split(':');
    final host = server[0];
    final port = int.parse(server[1]);

    _socket = await Socket.connect(host, port);
    _isRunning = true;

    print('Connected to Kafka broker at $_bootstrapServers');
  }

  Future<void> joinGroup() async {
    if (_socket == null) {
      throw Exception('Not connected to Kafka broker');
    }

    // Create a JoinGroupRequest
    final request = JoinGroupRequest(
      groupId: _groupId,
      sessionTimeoutMs: sessionTimeoutMs,
      rebalanceTimeoutMs: rebalanceTimeoutMs,
      protocols: [
        JoinGroupRequestProtocol(
          name: 'range', // Example protocol name
          metadata: Uint8List.fromList(utf8.encode('example-metadata')),
        ),
      ],
    );

    // Send the request to the broker
    _socket!.write(request.toBytes());
    await _socket!.flush();

    print('Sent JoinGroupRequest for group $_groupId');
  }

  Future<void> subscribe() async {
    if (_socket == null) {
      throw Exception('Not connected to Kafka broker');
    }

    await joinGroup();

    final request = {
      'group_id': _groupId,
      'topics': [_topic],
      'protocol_version': _protocolVersion,
    };

    _socket!.write(jsonEncode(request));
    await _socket!.flush();

    print(
        'Subscribed to topic $_topic with protocol version $_protocolVersion');
  }

  Stream<List<KafkaMessage>> consume() async* {
    if (_socket == null) {
      throw Exception('Not connected to Kafka broker');
    }

    final controller = StreamController<List<KafkaMessage>>();
    final subscription = _socket!.listen(
      (data) {
        final messages = _decodeMessages(data);
        controller.add(messages);
      },
      onError: (error) {
        controller.addError(error);
      },
      onDone: () {
        controller.close();
      },
    );

    yield* controller.stream;
  }

  List<KafkaMessage> _decodeMessages(List<int> data) {
    final messages = <KafkaMessage>[];
    final byteData = Uint8List.fromList(data);
    final byteBuffer = ByteData.sublistView(byteData);

    int offset = 0;
    while (offset < byteData.length) {
      final message = _decodeMessage(byteBuffer, offset);
      messages.add(message);
      offset += message.size;
    }

    return messages;
  }

  KafkaMessage _decodeMessage(ByteData byteBuffer, int offset) {
    switch (_protocolVersion) {
      case 0:
        return _decodeMessageV0(byteBuffer, offset);
      case 1:
        return _decodeMessageV1(byteBuffer, offset);
      case 2:
        return _decodeMessageV2(byteBuffer, offset);
      default:
        throw Exception(
            'Unsupported Kafka protocol version: $_protocolVersion');
    }
  }

  KafkaMessage _decodeMessageV0(ByteData byteBuffer, int offset) {
    final magicByte = byteBuffer.getUint8(offset);
    final attributes = byteBuffer.getUint8(offset + 1);
    final keyLength = byteBuffer.getInt32(offset + 2, Endian.big);
    final key = keyLength == -1
        ? null
        : utf8.decode(byteBuffer.buffer.asUint8List(offset + 6, keyLength));
    final valueLength = byteBuffer.getInt32(
        offset + 6 + (keyLength == -1 ? 0 : keyLength), Endian.big);
    final value = valueLength == -1
        ? null
        : utf8.decode(byteBuffer.buffer.asUint8List(
            offset + 10 + (keyLength == -1 ? 0 : keyLength), valueLength));

    return KafkaMessage(
      magicByte: magicByte,
      attributes: attributes,
      key: key,
      value: value,
      size: 14 +
          (keyLength == -1 ? 0 : keyLength) +
          (valueLength == -1 ? 0 : valueLength),
    );
  }

  KafkaMessage _decodeMessageV1(ByteData byteBuffer, int offset) {
    final magicByte = byteBuffer.getUint8(offset);
    final attributes = byteBuffer.getUint8(offset + 1);
    final timestamp = byteBuffer.getInt64(offset + 2, Endian.big);
    final keyLength = byteBuffer.getInt32(offset + 10, Endian.big);
    final key = keyLength == -1
        ? null
        : utf8.decode(byteBuffer.buffer.asUint8List(offset + 14, keyLength));
    final valueLength = byteBuffer.getInt32(
        offset + 14 + (keyLength == -1 ? 0 : keyLength), Endian.big);
    final value = valueLength == -1
        ? null
        : utf8.decode(byteBuffer.buffer.asUint8List(
            offset + 18 + (keyLength == -1 ? 0 : keyLength), valueLength));

    return KafkaMessage(
      magicByte: magicByte,
      attributes: attributes,
      key: key,
      value: value,
      timestamp: timestamp,
      size: 22 +
          (keyLength == -1 ? 0 : keyLength) +
          (valueLength == -1 ? 0 : valueLength),
    );
  }

  KafkaMessage _decodeMessageV2(ByteData byteBuffer, int offset) {
    final magicByte = byteBuffer.getUint8(offset);
    final attributes = byteBuffer.getUint8(offset + 1);
    final timestamp = byteBuffer.getInt64(offset + 2, Endian.big);
    final keyLength = byteBuffer.getInt32(offset + 10, Endian.big);
    final key = keyLength == -1
        ? null
        : utf8.decode(byteBuffer.buffer.asUint8List(offset + 14, keyLength));
    final valueLength = byteBuffer.getInt32(
        offset + 14 + (keyLength == -1 ? 0 : keyLength), Endian.big);
    final value = valueLength == -1
        ? null
        : utf8.decode(byteBuffer.buffer.asUint8List(
            offset + 18 + (keyLength == -1 ? 0 : keyLength), valueLength));

    final headers = _decodeHeaders(
        byteBuffer,
        offset +
            18 +
            (keyLength == -1 ? 0 : keyLength) +
            (valueLength == -1 ? 0 : valueLength));

    return KafkaMessage(
      magicByte: magicByte,
      attributes: attributes,
      key: key,
      value: value,
      timestamp: timestamp,
      headers: headers,
      size: 22 +
          (keyLength == -1 ? 0 : keyLength) +
          (valueLength == -1 ? 0 : valueLength) +
          headers.fold(0, (sum, header) => sum + header.size),
    );
  }

  List<KafkaHeader> _decodeHeaders(ByteData byteBuffer, int offset) {
    final headers = <KafkaHeader>[];
    final headerCount = byteBuffer.getInt32(offset, Endian.big);
    offset += 4;

    for (int i = 0; i < headerCount; i++) {
      final keyLength = byteBuffer.getInt16(offset, Endian.big);
      final key =
          utf8.decode(byteBuffer.buffer.asUint8List(offset + 2, keyLength));
      offset += 2 + keyLength;

      final valueLength = byteBuffer.getInt32(offset, Endian.big);
      final value = valueLength == -1
          ? null
          : utf8.decode(byteBuffer.buffer.asUint8List(offset + 4, valueLength));
      offset += 4 + (valueLength == -1 ? 0 : valueLength);

      headers.add(KafkaHeader(
          key: key,
          value: value,
          size: 6 + keyLength + (valueLength == -1 ? 0 : valueLength)));
    }

    return headers;
  }

  Future<void> close() async {
    _isRunning = false;
    await _socket?.close();
    print('Disconnected from Kafka broker');
  }
}

class KafkaMessage {
  final int magicByte;
  final int attributes;
  final String? key;
  final String? value;
  final int? timestamp;
  final List<KafkaHeader>? headers;
  final int size;

  KafkaMessage({
    required this.magicByte,
    required this.attributes,
    this.key,
    this.value,
    this.timestamp,
    this.headers,
    required this.size,
  });

  @override
  String toString() {
    return 'KafkaMessage{magicByte: $magicByte, attributes: $attributes, key: $key, value: $value, timestamp: $timestamp, headers: $headers, size: $size}';
  }
}

class KafkaHeader {
  final String key;
  final String? value;
  final int size;

  KafkaHeader({
    required this.key,
    this.value,
    required this.size,
  });

  @override
  String toString() {
    return 'KafkaHeader{key: $key, value: $value, size: $size}';
  }
}
