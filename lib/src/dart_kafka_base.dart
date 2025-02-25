import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

class KafkaException implements Exception {
  final String message;
  final int errorCode;

  KafkaException(this.message, this.errorCode);

  @override
  String toString() => 'KafkaException: $message (Error Code: $errorCode)';
}

class KafkaClient {
  final String _host;
  final int _port;
  Socket? _socket;
  String? _groupId;
  String? _memberId;
  Map<String, dynamic> _producerConfigurations;

  KafkaClient(
    this._host,
    this._port, {
    Map<String, dynamic> producerConfigurations = const {},
  }) : _producerConfigurations = producerConfigurations;

  Future<void> connect() async {
    try {
      _socket =
          await Socket.connect(_host, _port, timeout: Duration(seconds: 5));
    } on SocketException catch (e) {
      throw KafkaException(
          'Failed to connect to Kafka broker: ${e.message}', -1);
    } on TimeoutException catch (e) {
      throw KafkaException('Connection to Kafka broker timed out', -1);
    }
  }

  Future<void> disconnect() async {
    _socket?.destroy();
  }

  Future<void> produce(
    String topic,
    String message, {
    Map<String, dynamic>? configurations,
  }) async {
    if (_socket == null) {
      throw KafkaException('Not connected to Kafka broker', -1);
    }

    // Validate configurations
    if (configurations != null) {
      if (configurations['acks'] != null && configurations['acks'] is! int) {
        throw KafkaException(
            'Invalid configuration: acks must be an integer', -1);
      }
      // Add more validation rules as needed
    }

    try {
      final mergedConfigurations = {
        ..._producerConfigurations,
        ...?configurations,
      };

      final request =
          _createProduceRequest(topic, message, mergedConfigurations);
      _socket!.add(request);
      await _socket!.flush();
    } on SocketException catch (e) {
      throw KafkaException(
          'Network error while producing message: ${e.message}', -1);
    }
  }

  Uint8List _createProduceRequest(
    String topic,
    String message,
    Map<String, dynamic> configurations,
  ) {
    final builder = BytesBuilder();

    // API Key: Produce (0)
    builder.addByte(0);

    // API Version: 7
    builder.add([0, 7]);

    // Correlation ID: 1
    builder.add([0, 0, 0, 1]);

    // Client ID: "dart-kafka-client"
    final clientId =
        utf8.encode(configurations['client.id'] ?? 'dart-kafka-client');
    builder.add([0, 0]);
    builder.add([clientId.length.toUnsigned(16)]);
    builder.add(clientId);

    // Transactional ID: null (or provided in configurations)
    final transactionalId = configurations['transactional.id'];
    if (transactionalId != null) {
      final transactionalIdBytes = utf8.encode(transactionalId);
      builder.add([transactionalIdBytes.length.toUnsigned(16)]);
      builder.add(transactionalIdBytes);
    } else {
      builder.add([255, 255, 255, 255]); // Null
    }

    // Required Acks: -1 (all replicas) or provided in configurations
    final acks = configurations['acks'] ?? -1;
    builder.add(acks.toUnsigned(32));

    // Timeout: 30000 ms or provided in configurations
    final timeoutMs = configurations['request.timeout.ms'] ?? 30000;
    builder.add(timeoutMs.toUnsigned(32));

    // Topic Data
    builder.add([0, 0, 0, 1]); // 1 topic
    final topicBytes = utf8.encode(topic);
    builder.add([topicBytes.length.toUnsigned(16)]);
    builder.add(topicBytes);

    // Partition Data
    builder.add([0, 0, 0, 1]); // 1 partition
    builder.add([0, 0, 0, 0]); // Partition ID 0

    // Record Set
    final recordSet = _createRecordSet(message, configurations);
    builder.add([recordSet.length.toUnsigned(32)]);
    builder.add(recordSet);

    return builder.toBytes();
  }

  Uint8List _createRecordSet(
    String message,
    Map<String, dynamic> configurations,
  ) {
    final builder = BytesBuilder();

    // Record
    builder.add([0, 0, 0, 0, 0, 0, 0, 0]); // Offset
    builder.add([0, 0, 0, 0]); // Message Size
    builder.add([0, 0]); // CRC
    builder.add([0]); // Magic Byte

    // Attributes: Compression type (provided in configurations)
    final compressionType = configurations['compression.type'] ?? 'none';
    final compressionCode = _getCompressionCode(compressionType);
    builder.add([compressionCode]);

    // Timestamp: Current time or provided in configurations
    final timestamp =
        configurations['timestamp'] ?? DateTime.now().millisecondsSinceEpoch;
    builder.add(timestamp.toUnsigned(64));

    // Key: null or provided in configurations
    final key = configurations['key'];
    if (key != null) {
      final keyBytes = utf8.encode(key);
      builder.add([keyBytes.length.toUnsigned(32)]);
      builder.add(keyBytes);
    } else {
      builder.add([255, 255, 255, 255]); // Null
    }

    // Value: message
    final messageBytes = utf8.encode(message);
    builder.add([messageBytes.length.toUnsigned(32)]);
    builder.add(messageBytes);

    // Headers: empty or provided in configurations
    final headers = configurations['headers'] ?? {};
    builder.add(headers.length.toUnsigned(32));
    for (final entry in headers.entries) {
      final keyBytes = utf8.encode(entry.key);
      final valueBytes = utf8.encode(entry.value);
      builder.add([keyBytes.length.toUnsigned(16)]);
      builder.add(keyBytes);
      builder.add([valueBytes.length.toUnsigned(16)]);
      builder.add(valueBytes);
    }

    return builder.toBytes();
  }

  int _getCompressionCode(String compressionType) {
    switch (compressionType) {
      case 'none':
        return 0;
      case 'gzip':
        return 1;
      case 'snappy':
        return 2;
      case 'lz4':
        return 3;
      case 'zstd':
        return 4;
      default:
        throw KafkaException(
            'Unsupported compression type: $compressionType', -1);
    }
  }

  Future<void> consume(String topic) async {
    if (_socket == null) {
      throw KafkaException('Not connected to Kafka broker', -1);
    }

    try {
      final request = _createFetchRequest(topic);
      _socket!.add(request);
      await _socket!.flush();

      final response = await _socket!.fold<Uint8List>(
        Uint8List(0),
        (previous, element) => Uint8List.fromList([...previous, ...element]),
      );

      _processFetchResponse(response);
    } on SocketException catch (e) {
      throw KafkaException(
          'Network error while consuming messages: ${e.message}', -1);
    }
  }

  Uint8List _createFetchRequest(String topic) {
    final builder = BytesBuilder();

    // API Key: Fetch (1)
    builder.addByte(1);

    // API Version: 11
    builder.add([0, 11]);

    // Correlation ID: 2
    builder.add([0, 0, 0, 2]);

    // Client ID: "dart-kafka-client"
    final clientId = utf8.encode('dart-kafka-client');
    builder.add([0, 0]);
    builder.add([clientId.length.toUnsigned(16)]);
    builder.add(clientId);

    // Replica ID: -1
    builder.add([255, 255, 255, 255]);

    // Max Wait Time: 5000 ms
    builder.add([0, 0, 19, 136]);

    // Min Bytes: 1
    builder.add([0, 0, 0, 1]);

    // Max Bytes: 1048576
    builder.add([0, 15, 255, 255]);

    // Isolation Level: 0
    builder.add([0]);

    // Session ID: 0
    builder.add([0, 0, 0, 0]);

    // Session Epoch: -1
    builder.add([255, 255, 255, 255]);

    // Topic Data
    builder.add([0, 0, 0, 1]); // 1 topic
    final topicBytes = utf8.encode(topic);
    builder.add([topicBytes.length.toUnsigned(16)]);
    builder.add(topicBytes);

    // Partition Data
    builder.add([0, 0, 0, 1]); // 1 partition
    builder.add([0, 0, 0, 0]); // Partition ID 0
    builder.add([0, 0, 0, 0]); // Current Leader Epoch
    builder.add([0, 0, 0, 0]); // Fetch Offset
    builder.add([0, 0, 0, 0]); // Log Start Offset
    builder.add([0, 0, 0, 1]); // Partition Max Bytes

    return builder.toBytes();
  }

  void _processFetchResponse(Uint8List response) {
    // Assuming the error code is at a specific position in the response
    final errorCode = ByteData.sublistView(response.sublist(4, 8)).getInt32(0);
    if (errorCode != 0) {
      throw KafkaException('Failed to fetch messages', errorCode);
    }

    // Process the response and print the messages
    print('Received response: ${utf8.decode(response)}');
  }

  Future<void> subscribe(
    String groupId,
    List<String> topics, {
    Map<String, dynamic> configurations = const {},
  }) async {
    if (_socket == null) {
      throw KafkaException('Not connected to Kafka broker', -1);
    }

    _groupId = groupId;

    try {
      // Step 1: Join Group
      final joinGroupRequest = _createJoinGroupRequest(
        groupId,
        topics,
        configurations: configurations,
      );
      _socket!.add(joinGroupRequest);
      await _socket!.flush();

      final joinGroupResponse = await _socket!.fold<Uint8List>(
        Uint8List(0),
        (previous, element) => Uint8List.fromList([...previous, ...element]),
      );

      _memberId = _processJoinGroupResponse(joinGroupResponse);

      // Step 2: Sync Group
      final syncGroupRequest = _createSyncGroupRequest(groupId, _memberId!);
      _socket!.add(syncGroupRequest);
      await _socket!.flush();

      final syncGroupResponse = await _socket!.fold<Uint8List>(
        Uint8List(0),
        (previous, element) => Uint8List.fromList([...previous, ...element]),
      );

      _processSyncGroupResponse(syncGroupResponse);
    } on SocketException catch (e) {
      throw KafkaException('Network error while subscribing: ${e.message}', -1);
    }
  }

  Uint8List _createJoinGroupRequest(
    String groupId,
    List<String> topics, {
    required Map<String, dynamic> configurations,
  }) {
    final builder = BytesBuilder();

    // API Key: JoinGroup (11)
    builder.addByte(11);

    // API Version: 5
    builder.add([0, 5]);

    // Correlation ID: 3
    builder.add([0, 0, 0, 3]);

    // Client ID: "dart-kafka-client"
    final clientId = utf8.encode('dart-kafka-client');
    builder.add([0, 0]);
    builder.add([clientId.length.toUnsigned(16)]);
    builder.add(clientId);

    // Group ID
    final groupIdBytes = utf8.encode(groupId);
    builder.add([groupIdBytes.length.toUnsigned(16)]);
    builder.add(groupIdBytes);

    // Session Timeout
    final sessionTimeoutMs = configurations['session.timeout.ms'] ?? 10000;
    builder.add(sessionTimeoutMs.toUnsigned(32));

    // Rebalance Timeout
    final rebalanceTimeoutMs = configurations['rebalance.timeout.ms'] ?? 30000;
    builder.add(rebalanceTimeoutMs.toUnsigned(32));

    // Member ID: "" (empty string for new member)
    builder.add([0, 0]);

    // Protocol Type: "consumer"
    final protocolType = utf8.encode('consumer');
    builder.add([protocolType.length.toUnsigned(16)]);
    builder.add(protocolType);

    // Group Protocols
    builder.add([0, 0, 0, 1]); // 1 protocol

    // Protocol Name: "roundrobin"
    final protocolName = utf8.encode('roundrobin');
    builder.add([protocolName.length.toUnsigned(16)]);
    builder.add(protocolName);

    // Protocol Metadata
    final metadata = _createConsumerProtocolMetadata(
      topics,
      configurations: configurations,
    );
    builder.add([metadata.length.toUnsigned(32)]);
    builder.add(metadata);

    return builder.toBytes();
  }

  Uint8List _createConsumerProtocolMetadata(
    List<String> topics, {
    required Map<String, dynamic> configurations,
  }) {
    final builder = BytesBuilder();

    // Version: 0
    builder.add([0, 0]);

    // Topics
    builder.add([topics.length.toUnsigned(16)]);
    for (final topic in topics) {
      final topicBytes = utf8.encode(topic);
      builder.add([topicBytes.length.toUnsigned(16)]);
      builder.add(topicBytes);
    }

    // User Data: Include configurations
    final userData = configurations;
    final userDataBytes = utf8.encode(userData.toString());
    builder.add([userDataBytes.length.toUnsigned(32)]);
    builder.add(userDataBytes);

    return builder.toBytes();
  }

  String _processJoinGroupResponse(Uint8List response) {
    // Extract Member ID from the response
    final errorCode = ByteData.sublistView(response.sublist(4, 8)).getInt32(0);
    if (errorCode != 0) {
      throw KafkaException('Failed to join group', errorCode);
    }

    final memberIdLength = response[8] << 8 | response[9];
    final memberId = utf8.decode(response.sublist(10, 10 + memberIdLength));
    return memberId;
  }

  Uint8List _createSyncGroupRequest(String groupId, String memberId) {
    final builder = BytesBuilder();

    // API Key: SyncGroup (14)
    builder.addByte(14);

    // API Version: 3
    builder.add([0, 3]);

    // Correlation ID: 4
    builder.add([0, 0, 0, 4]);

    // Client ID: "dart-kafka-client"
    final clientId = utf8.encode('dart-kafka-client');
    builder.add([0, 0]);
    builder.add([clientId.length.toUnsigned(16)]);
    builder.add(clientId);

    // Group ID
    final groupIdBytes = utf8.encode(groupId);
    builder.add([groupIdBytes.length.toUnsigned(16)]);
    builder.add(groupIdBytes);

    // Generation ID: 1
    builder.add([0, 0, 0, 1]);

    // Member ID
    final memberIdBytes = utf8.encode(memberId);
    builder.add([memberIdBytes.length.toUnsigned(16)]);
    builder.add(memberIdBytes);

    // Group Assignments: Empty (for now)
    builder.add([0, 0, 0, 0]);

    return builder.toBytes();
  }

  void _processSyncGroupResponse(Uint8List response) {
    // Process the response and print the assignments
    final errorCode = ByteData.sublistView(response.sublist(4, 8)).getInt32(0);
    if (errorCode != 0) {
      throw KafkaException('Failed to sync group', errorCode);
    }

    print('Received SyncGroup response: ${utf8.decode(response)}');
  }

  // Offset Control Methods

  Future<Map<String, int>> fetchOffsets(String topic, int partition) async {
    if (_socket == null) {
      throw KafkaException('Not connected to Kafka broker', -1);
    }

    try {
      final request = _createListOffsetsRequest(topic, partition);
      _socket!.add(request);
      await _socket!.flush();

      final response = await _socket!.fold<Uint8List>(
        Uint8List(0),
        (previous, element) => Uint8List.fromList([...previous, ...element]),
      );

      return _processListOffsetsResponse(response);
    } on SocketException catch (e) {
      throw KafkaException(
          'Network error while fetching offsets: ${e.message}', -1);
    }
  }

  Uint8List _createListOffsetsRequest(String topic, int partition) {
    final builder = BytesBuilder();

    // API Key: ListOffsets (2)
    builder.addByte(2);

    // API Version: 5
    builder.add([0, 5]);

    // Correlation ID: 5
    builder.add([0, 0, 0, 5]);

    // Client ID: "dart-kafka-client"
    final clientId = utf8.encode('dart-kafka-client');
    builder.add([0, 0]);
    builder.add([clientId.length.toUnsigned(16)]);
    builder.add(clientId);

    // Replica ID: -1
    builder.add([255, 255, 255, 255]);

    // Isolation Level: 0 (read_uncommitted)
    builder.add([0]);

    // Topic Data
    builder.add([0, 0, 0, 1]); // 1 topic
    final topicBytes = utf8.encode(topic);
    builder.add([topicBytes.length.toUnsigned(16)]);
    builder.add(topicBytes);

    // Partition Data
    builder.add([0, 0, 0, 1]); // 1 partition
    builder.add([0, 0, 0, partition]); // Partition ID
    builder.add([0, 0, 0, 0]); // Timestamp (-1 for latest offset)
    builder.add([0, 0, 0, 1]); // Max Num Offsets

    return builder.toBytes();
  }

  Map<String, int> _processListOffsetsResponse(Uint8List response) {
    // Extract offsets from the response
    final errorCode = ByteData.sublistView(response.sublist(4, 8)).getInt32(0);
    if (errorCode != 0) {
      throw KafkaException('Failed to fetch offsets', errorCode);
    }

    final offset = response.sublist(16, 24);
    final offsetValue = ByteData.sublistView(offset).getInt64(0);

    return {
      'offset': offsetValue,
    };
  }

  Future<void> commitOffset(String topic, int partition, int offset) async {
    if (_socket == null) {
      throw KafkaException('Not connected to Kafka broker', -1);
    }

    try {
      final request = _createOffsetCommitRequest(topic, partition, offset);
      _socket!.add(request);
      await _socket!.flush();

      final response = await _socket!.fold<Uint8List>(
        Uint8List(0),
        (previous, element) => Uint8List.fromList([...previous, ...element]),
      );

      _processOffsetCommitResponse(response);
    } on SocketException catch (e) {
      throw KafkaException(
          'Network error while committing offset: ${e.message}', -1);
    }
  }

  Uint8List _createOffsetCommitRequest(
      String topic, int partition, int offset) {
    final builder = BytesBuilder();

    // API Key: OffsetCommit (8)
    builder.addByte(8);

    // API Version: 6
    builder.add([0, 6]);

    // Correlation ID: 6
    builder.add([0, 0, 0, 6]);

    // Client ID: "dart-kafka-client"
    final clientId = utf8.encode('dart-kafka-client');
    builder.add([0, 0]);
    builder.add([clientId.length.toUnsigned(16)]);
    builder.add(clientId);

    // Group ID
    final groupIdBytes = utf8.encode(_groupId!);
    builder.add([groupIdBytes.length.toUnsigned(16)]);
    builder.add(groupIdBytes);

    // Generation ID: 1
    builder.add([0, 0, 0, 1]);

    // Member ID
    final memberIdBytes = utf8.encode(_memberId!);
    builder.add([memberIdBytes.length.toUnsigned(16)]);
    builder.add(memberIdBytes);

    // Retention Time: -1 (use broker configuration)
    builder.add([255, 255, 255, 255]);

    // Topic Data
    builder.add([0, 0, 0, 1]); // 1 topic
    final topicBytes = utf8.encode(topic);
    builder.add([topicBytes.length.toUnsigned(16)]);
    builder.add(topicBytes);

    // Partition Data
    builder.add([0, 0, 0, 1]); // 1 partition
    builder.add([0, 0, 0, partition]); // Partition ID
    builder.add([0, 0, 0, 0, 0, 0, 0, offset]); // Offset
    builder.add([0, 0]); // Metadata

    return builder.toBytes();
  }

  void _processOffsetCommitResponse(Uint8List response) {
    // Process the response and print the result
    final errorCode = ByteData.sublistView(response.sublist(4, 8)).getInt32(0);
    if (errorCode != 0) {
      throw KafkaException('Failed to commit offset', errorCode);
    }

    print('Offset commit response: ${utf8.decode(response)}');
  }

  Future<void> seekToOffset(String topic, int partition, int offset) async {
    if (_socket == null) {
      throw KafkaException('Not connected to Kafka broker', -1);
    }

    try {
      final request = _createOffsetFetchRequest(topic, partition);
      _socket!.add(request);
      await _socket!.flush();

      final response = await _socket!.fold<Uint8List>(
        Uint8List(0),
        (previous, element) => Uint8List.fromList([...previous, ...element]),
      );

      _processOffsetFetchResponse(response);

      // Update the fetch offset to the desired position
      final fetchRequest = _createFetchRequest(topic);
      _socket!.add(fetchRequest);
      await _socket!.flush();
    } on SocketException catch (e) {
      throw KafkaException(
          'Network error while seeking to offset: ${e.message}', -1);
    }
  }

  Uint8List _createOffsetFetchRequest(String topic, int partition) {
    final builder = BytesBuilder();

    // API Key: OffsetFetch (9)
    builder.addByte(9);

    // API Version: 5
    builder.add([0, 5]);

    // Correlation ID: 7
    builder.add([0, 0, 0, 7]);

    // Client ID: "dart-kafka-client"
    final clientId = utf8.encode('dart-kafka-client');
    builder.add([0, 0]);
    builder.add([clientId.length.toUnsigned(16)]);
    builder.add(clientId);

    // Group ID
    final groupIdBytes = utf8.encode(_groupId!);
    builder.add([groupIdBytes.length.toUnsigned(16)]);
    builder.add(groupIdBytes);

    // Topic Data
    builder.add([0, 0, 0, 1]); // 1 topic
    final topicBytes = utf8.encode(topic);
    builder.add([topicBytes.length.toUnsigned(16)]);
    builder.add(topicBytes);

    // Partition Data
    builder.add([0, 0, 0, 1]); // 1 partition
    builder.add([0, 0, 0, partition]); // Partition ID

    return builder.toBytes();
  }

  void _processOffsetFetchResponse(Uint8List response) {
    // Process the response and print the fetched offset
    final errorCode = ByteData.sublistView(response.sublist(4, 8)).getInt32(0);
    if (errorCode != 0) {
      throw KafkaException('Failed to fetch offset', errorCode);
    }

    print('Offset fetch response: ${utf8.decode(response)}');
  }
}
