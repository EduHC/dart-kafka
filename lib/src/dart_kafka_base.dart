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
  String? _generationId;
  Map<String, dynamic> _producerConfigurations;
  bool _isStaticMember = false;
  final CorrelationIdGenerator correlationIdGenerator =
      CorrelationIdGenerator();

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
    if (_socket != null) {
      if (_groupId != null && _memberId != null) {
        await _leaveGroup();
      }
      _socket!.destroy();
    }
  }

  Future<void> _sendHeartbeat() async {
    if (_socket == null ||
        _groupId == null ||
        _memberId == null ||
        _generationId == null) {
      throw KafkaException('Not connected or not part of a consumer group', -1);
    }

    try {
      final request = _createHeartbeatRequest();
      _socket!.add(request);
      await _socket!.flush();

      final response = await _socket!.fold<Uint8List>(
        Uint8List(0),
        (previous, element) => Uint8List.fromList([...previous, ...element]),
      );

      _processHeartbeatResponse(response);
    } on SocketException catch (e) {
      throw KafkaException(
          'Network error while sending heartbeat: ${e.message}', -1);
    }
  }

  Uint8List _createHeartbeatRequest() {
    final builder = BytesBuilder();

    // API Key: Heartbeat (12)
    builder.addByte(12);

    // API Version: 4
    builder.add([0, 4]);

    // Correlation ID: 8
    builder.add([0, 0, 0, 8]);

    // Client ID: "dart-kafka-client"
    final clientId = utf8.encode('dart-kafka-client');
    builder.add([0, 0]);
    builder.add([clientId.length.toUnsigned(16)]);
    builder.add(clientId);

    // Group ID
    final groupIdBytes = utf8.encode(_groupId!);
    builder.add([groupIdBytes.length.toUnsigned(16)]);
    builder.add(groupIdBytes);

    // Generation ID
    builder.add([int.parse(_generationId!).toUnsigned(32)]);

    // Member ID
    final memberIdBytes = utf8.encode(_memberId!);
    builder.add([memberIdBytes.length.toUnsigned(16)]);
    builder.add(memberIdBytes);

    return builder.toBytes();
  }

  void _processHeartbeatResponse(Uint8List response) {
    final errorCode = ByteData.sublistView(response.sublist(4, 8)).getInt32(0);
    if (errorCode != 0) {
      throw KafkaException('Failed to send heartbeat', errorCode);
    }

    print('Heartbeat successful');
  }

  Future<void> _leaveGroup() async {
    if (_socket == null || _groupId == null || _memberId == null) {
      throw KafkaException('Not connected or not part of a consumer group', -1);
    }

    try {
      final request = _createLeaveGroupRequest();
      _socket!.add(request);
      await _socket!.flush();

      final response = await _socket!.fold<Uint8List>(
        Uint8List(0),
        (previous, element) => Uint8List.fromList([...previous, ...element]),
      );

      _processLeaveGroupResponse(response);
    } on SocketException catch (e) {
      throw KafkaException(
          'Network error while leaving group: ${e.message}', -1);
    }
  }

  Uint8List _createLeaveGroupRequest() {
    final builder = BytesBuilder();

    // API Key: LeaveGroup (13)
    builder.addByte(13);

    // API Version: 3
    builder.add([0, 3]);

    // Correlation ID: 9
    builder.add([0, 0, 0, 9]);

    // Client ID: "dart-kafka-client"
    final clientId = utf8.encode('dart-kafka-client');
    builder.add([0, 0]);
    builder.add([clientId.length.toUnsigned(16)]);
    builder.add(clientId);

    // Group ID
    final groupIdBytes = utf8.encode(_groupId!);
    builder.add([groupIdBytes.length.toUnsigned(16)]);
    builder.add(groupIdBytes);

    // Member ID
    final memberIdBytes = utf8.encode(_memberId!);
    builder.add([memberIdBytes.length.toUnsigned(16)]);
    builder.add(memberIdBytes);

    return builder.toBytes();
  }

  void _processLeaveGroupResponse(Uint8List response) {
    final errorCode = ByteData.sublistView(response.sublist(4, 8)).getInt32(0);
    if (errorCode != 0) {
      throw KafkaException('Failed to leave group', errorCode);
    }

    print('Successfully left the consumer group');
  }

  // Static membership support
  void enableStaticMembership(String memberId) {
    _isStaticMember = true;
    _memberId = memberId;
  }

  void _startHeartbeat() {
    Future.doWhile(() async {
      await _sendHeartbeat();
      await Future.delayed(Duration(seconds: 3));
      return true;
    });
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
    _buildBaseHeader(
      builder,
      0, // API Key for Produce
      7, // API Version
      configurations['client.id'] ?? 'dart-kafka-client',
    );

    final transactionalId = configurations['transactional.id'];
    if (transactionalId != null) {
      final transactionalIdBytes = utf8.encode(transactionalId);
      builder.add(_int16(transactionalIdBytes.length));
      builder.add(transactionalIdBytes);
    } else {
      builder.add([0xFF, 0xFF]); // Null Transactional ID
    }

    // Required Acks (Int16)
    final acks = configurations['acks'] ?? -1;
    builder.add(_int16(acks));

    // Timeout (Int32)
    final timeoutMs = configurations['request.timeout.ms'] ?? 30000;
    builder.add(_int32(timeoutMs));

    // Topic Data
    final topicBytes = utf8.encode(topic);
    builder.add(_int16(topicBytes.length)); // Topic name length (Int16)
    builder.add(topicBytes);

    // Partition Data
    builder.add([0, 0, 0, 1]); // Number of partitions (Int32)
    builder.add([0, 0, 0, 0]); // Partition ID (Int32)

    // Record Set
    final recordSet = _createRecordSet(message, configurations);
    builder.add(_int32(recordSet.length)); // Record batch size (Int32)
    builder.add(recordSet);

    return builder.toBytes();
  }

  Uint8List _createRecordSet(
    String message,
    Map<String, dynamic> configurations,
  ) {
    final builder = BytesBuilder();

    // Kafka Record Batch Header (Mandatory Fields)
    builder.add(
        [0, 0, 0, 0, 0, 0, 0, 0]); // Base Offset (Int64, always 0 in request)
    final recordData = BytesBuilder();

    // Mandatory Kafka Batch Fields
    recordData.add(_int32(0)); // Partition Leader Epoch (Int32)
    recordData.add([2]); // Magic Byte (Int8, v2)
    recordData.add(_int32(0)); // Placeholder for CRC (Int32, calculated later)
    recordData.add(_int16(0)); // Attributes (Int16)
    recordData.add(_int32(1)); // Last Offset Delta (Int32)
    recordData.add(_int64(
        DateTime.now().millisecondsSinceEpoch)); // First Timestamp (Int64)
    recordData.add(
        _int64(DateTime.now().millisecondsSinceEpoch)); // Max Timestamp (Int64)
    recordData.add(_int64(0)); // Producer ID (Int64)
    recordData.add(_int16(0)); // Producer Epoch (Int16)
    recordData.add(_int32(0)); // Base Sequence (Int32)

    // Single Record Inside the Batch
    final messageBytes = utf8.encode(message);
    recordData.add(_varInt(messageBytes.length + 5)); // Length (VarInt)
    recordData.add([0]); // Attributes (Int8)
    recordData.add([0]); // Timestamp Delta (VarInt)
    recordData.add([0]); // Offset Delta (VarInt)
    recordData.add([0]); // Key Length (Null)
    recordData.add(_varInt(messageBytes.length)); // Message Length (VarInt)
    recordData.add(messageBytes);

    // Compute CRC32 over batch (excluding Magic Byte and CRC field itself)
    final recordBytes = recordData.toBytes();
    final crc = _computeCRC32(recordBytes.sublist(5)); // Skip Magic Byte and CRC field
    recordBytes.setRange(5, 9, _int32(crc)); // Insert CRC value

    // Add Record Batch Length
    builder.add(_int32(recordBytes.length));
    builder.add(recordBytes);

    return builder.toBytes();
  }

  Future<void> consume(String topic) async {
    if (_socket == null) {
      throw KafkaException('Not connected to Kafka broker', -1);
    }

    try {
      final request = _createFetchRequest(topic);
      _socket!.add(request);
      await _socket!.flush();

      await _socket!.listen((event) => print("Recebido: $event"),
          onError: (err) => print("Erro: $err"));

      // final response = await _socket!
      //     .fold<Uint8List>(
      //       Uint8List(0),
      //       (previous, element) =>
      //           Uint8List.fromList([...previous, ...element]),
      //     )
      //     .timeout(Duration(minutes: 1));

      // _processFetchResponse(response);
    } on SocketException catch (e) {
      throw KafkaException(
          'Network error while consuming messages: ${e.message}', -1);
    }
  }

  Uint8List _createFetchRequest(String topic) {
    final builder = BytesBuilder();
    _buildBaseHeader(builder, 1, 7, 'dart-kafka-client');

    // Replica ID: -1
    builder.add([255, 255, 255, 255]);

    // Max Wait Time: 5000 ms
    builder.add([0, 0, 19, 136]);

    // Min Bytes: 1
    builder.add([0, 0, 0, 1]);

    // Max Bytes: 1048576
    builder.add([0x00, 0x10, 0x00, 0x00]);

    // Isolation Level: 0
    builder.add([0]);

    // Session ID: 0
    builder.add([0, 0, 0, 0]);

    // Session Epoch: -1
    builder.add([255, 255, 255, 255]);

    // Topic Data
    // builder.add([0, 0, 0, 1]); // Topics Count

    final topicBytes = utf8.encode(topic);
    ByteData _2ByteData = ByteData(2);
    _2ByteData.setUint16(0, topicBytes.length);
    builder.add(_2ByteData.buffer.asUint8List().toList(growable: false));
    builder.add(topicBytes);

    // Partition Data
    // builder.add([0, 0, 0, 1]); // Partitions Count
    builder.add([0, 0, 0, 0]); // Partition ID 0
    // builder.add([0xFF, 0xFF, 0xFF, 0xFF]); // Current Leader Epoch
    builder.add([0, 0, 0, 0, 0, 0, 0, 0]); // Fetch Offset
    builder.add(
        [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]); // Log Start Offset
    builder.add([0xFF, 0x10, 0x00, 0x00]); // Partition Max Bytes

    // forgotten_topics_data
    // builder.add(bytes)

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

      final syncGroupRequest = _createSyncGroupRequest(groupId, _memberId!);
      _socket!.add(syncGroupRequest);
      await _socket!.flush();

      final syncGroupResponse = await _socket!.fold<Uint8List>(
        Uint8List(0),
        (previous, element) => Uint8List.fromList([...previous, ...element]),
      );

      _processSyncGroupResponse(syncGroupResponse);

      // Start sending heartbeats
      _startHeartbeat();
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
    _buildBaseHeader(builder, 11, 7, 'dart-kafka-client');

    // Group ID
    final groupIdBytes = utf8.encode(groupId);
    ByteData _2ByteData = ByteData(2);
    _2ByteData.setUint16(0, groupIdBytes.length);
    builder.add(_2ByteData.buffer.asUint8List().toList(growable: false));
    builder.add(groupIdBytes);

    // Session Timeout
    final sessionTimeoutMs = configurations['session.timeout.ms'] ?? 10000;
    ByteData _4byteData = ByteData(4);
    _4byteData.setUint32(0, sessionTimeoutMs);
    builder.add(_4byteData.buffer.asUint8List().toList(growable: false));

    // Rebalance Timeout
    final rebalanceTimeoutMs = configurations['rebalance.timeout.ms'] ?? 30000;
    _4byteData.setUint32(0, rebalanceTimeoutMs);
    builder.add(_4byteData.buffer.asUint8List().toList(growable: false));

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

  String? _processJoinGroupResponse(Uint8List response) {
    // Extract Member ID from the response
    if (response.isEmpty) return null;
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

  void _buildBaseHeader(
      BytesBuilder builder, int apiKey, int apiVersion, String clientId) {
    builder.add(_int16(apiKey));
    builder.add(_int16(apiVersion));

    // generate the CorrelationId
    List<int> correlationId = correlationIdGenerator.generate();
    builder.add(correlationId);

    // transform clientId to byte-level
    Uint8List clientIdBytes = utf8.encode(clientId);
    ByteData clientIdByteData = ByteData(2);
    clientIdByteData.setUint16(0, clientIdBytes.length);
    List<int> clientIdBytesLength =
        clientIdByteData.buffer.asUint8List().toList(growable: false);
    builder.add(clientIdBytesLength);
    builder.add(clientIdBytes);
  }

  /// Helper to encode Int16 in Big Endian
  Uint8List _int16(int value) {
    final data = ByteData(2);
    data.setInt16(0, value, Endian.big);
    return data.buffer.asUint8List();
  }

  /// Helper to encode Int32 in Big Endian
  Uint8List _int32(int value) {
    final data = ByteData(4);
    data.setInt32(0, value, Endian.big);
    return data.buffer.asUint8List();
  }

  /// Helper to encode Int64 in Big Endian
  Uint8List _int64(int value) {
    final data = ByteData(8);
    data.setInt64(0, value, Endian.big);
    return data.buffer.asUint8List();
  }

  /// Encode VarInt (Kafka uses variable-length encoding)
  Uint8List _varInt(int value) {
    final buffer = BytesBuilder();
    while ((value & ~0x7F) != 0) {
      buffer.addByte((value & 0x7F) | 0x80);
      value >>= 7;
    }
    buffer.addByte(value);
    return buffer.toBytes();
  }

  /// Compute CRC32 checksum
  int _computeCRC32(Uint8List data) {
    const int polynomial = 0x82F63B78; // Reversed representation of 0x1EDC6F41

    // Initialize CRC value
    int crc = 0xFFFFFFFF;

    // Process each byte in the input data
    for (final byte in data) {
      crc ^= byte;
      for (int i = 0; i < 8; i++) {
        if (crc & 1 == 1) {
          crc = (crc >> 1) ^ polynomial;
        } else {
          crc = crc >> 1;
        }
      }
    }

    return crc ^ 0xFFFFFFFF;
  }
}

class CorrelationIdGenerator {
  static int _lastTimestamp = 0;
  static int _counter = 0;

  static int next() {
    int now = DateTime.now().millisecondsSinceEpoch;

    if (now == _lastTimestamp) {
      _counter++;
    } else {
      _counter = 0;
      _lastTimestamp = now;
    }

    return (now << 16) | (_counter & 0xFFFF);
  }

  static Uint8List correlationIdToList(int correlationId) {
    ByteData byteData = ByteData(4);
    byteData.setUint32(0, correlationId, Endian.big);

    return byteData.buffer.asUint8List();
  }

  List<int> generate() {
    return correlationIdToList(next()).toList(growable: false);
  }
}
