import 'dart:convert';
import 'dart:typed_data';

import 'package:dart_kafka/src/kafka_utils.dart';

class JoinGroupRequest {
  final String groupId;
  final int sessionTimeoutMs;
  final int rebalanceTimeoutMs;
  final String memberId;
  final String protocolType;
  final List<JoinGroupRequestProtocol> protocols;
  final KafkaUtils utils = KafkaUtils();

  JoinGroupRequest({
    required this.groupId,
    required this.sessionTimeoutMs,
    required this.rebalanceTimeoutMs,
    this.memberId = '',
    this.protocolType = 'consumer',
    required this.protocols,
  });

  /// Converts the request to a byte array according to the Kafka protocol.
  Uint8List toBytes() {
    final buffer = BytesBuilder();

    // Add groupId (string)
    final groupIdBytes = utf8.encode(groupId);
    buffer.add(utils.int32(groupIdBytes.length));
    buffer.add(groupIdBytes);

    // Add sessionTimeoutMs (int32)
    buffer.add(utils.int32(sessionTimeoutMs));

    // Add rebalanceTimeoutMs (int32)
    buffer.add(utils.int32(rebalanceTimeoutMs));

    // Add memberId (string)
    final memberIdBytes = utf8.encode(memberId);
    buffer.add(utils.int32(memberIdBytes.length));
    buffer.add(memberIdBytes);

    // Add protocolType (string)
    final protocolTypeBytes = utf8.encode(protocolType);
    buffer.add(utils.int32(protocolTypeBytes.length));
    buffer.add(protocolTypeBytes);

    // Add protocols (array)
    buffer.add(utils.int32(protocols.length));
    for (final protocol in protocols) {
      buffer.add(protocol.toBytes());
    }

    return buffer.toBytes();
  }
}

class JoinGroupRequestProtocol {
  final String name;
  final Uint8List metadata;
  final KafkaUtils utils = KafkaUtils();

  JoinGroupRequestProtocol({
    required this.name,
    required this.metadata,
  });

  /// Converts the protocol to a byte array according to the Kafka protocol.
  Uint8List toBytes() {
    final buffer = BytesBuilder();

    // Add name (string)
    final nameBytes = utf8.encode(name);
    buffer.add(utils.int32(nameBytes.length));
    buffer.add(nameBytes);

    // Add metadata (bytes)
    buffer.add(utils.int32(metadata.length));
    buffer.add(metadata);

    return buffer.toBytes();
  }
}
