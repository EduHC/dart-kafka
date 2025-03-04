import 'dart:typed_data';

import 'package:dart_kafka/src/models/broker.dart';
import 'package:dart_kafka/src/models/metadata/kafka_partition_metadata.dart';
import 'package:dart_kafka/src/models/metadata/kafka_topic_metadata.dart';
import 'package:dart_kafka/src/protocol/utils.dart';
import 'package:dart_kafka/src/models/responses/metadata_response.dart';
import 'package:dart_kafka/src/protocol/apis.dart';

class KafkaMetadataApi {
  final int apiKey;
  final Utils utils = Utils();

  KafkaMetadataApi() : apiKey = METADATA;

  /// Method to serialize to build and serialize the MetadataRequest to Byte Array
  /// Default value of allowAutoTopicCreation is FALSE
  Uint8List serialize(
      {required int correlationId,
      String? clientId,
      required List<String> topics,
      required int apiVersion,
      bool allowAutoTopicCreation = false,
      bool includeClusterAuthorizedOperations = false,
      bool includeTopicAuthorizedOperations = false}) {
    final byteBuffer = BytesBuilder();
    byteBuffer.add(utils.int16(apiKey));
    byteBuffer.add(utils.int16(apiVersion));
    byteBuffer.add(utils.int32(correlationId));

    if (clientId != null) {
      final clientIdBytes = clientId.codeUnits;
      byteBuffer.add(utils.int16(clientIdBytes.length));
      byteBuffer.add(clientIdBytes);
    } else {
      byteBuffer.add(utils.int16(-1));
    }

    byteBuffer.add(utils.int32(topics.length));
    for (var topic in topics) {
      final topicBytes = topic.codeUnits;
      byteBuffer.add(utils.int16(topicBytes.length));
      byteBuffer.add(topicBytes);
    }

    if (apiVersion >= 4) {
      byteBuffer.add(utils.int8(allowAutoTopicCreation ? 1 : 0));
    }

    if (apiVersion >= 8) {
      if (apiVersion < 11) {
        byteBuffer.add(utils.int8(includeClusterAuthorizedOperations ? 1 : 0));
      }
      byteBuffer.add(utils.int8(includeTopicAuthorizedOperations ? 1 : 0));
    }

    Uint8List message = byteBuffer.toBytes();
    return Uint8List.fromList([...utils.int32(message.length), ...message]);
  }

  /// Method to deserialize the MetadataResponse from a Byte Array
  dynamic deserialize(Uint8List data, int apiVersion) {
    if (data.length < 14) {
      print("Invalid byte array: Insufficient length for MetadataResponse");
      return null;
    }

    final buffer = ByteData.sublistView(data);
    int offset = 0;

    switch (apiVersion) {
      case 0:
        return _deserialize0(
            buffer: buffer, offset: offset, messageLength: data.length);
      case 1:
        return _deserialize1(
            buffer: buffer, offset: offset, messageLength: data.length);
      case 2:
        return _deserialize2(
            buffer: buffer, offset: offset, messageLength: data.length);
      case 3:
      case 4:
        return _deserialize3(
            buffer: buffer, offset: offset, messageLength: data.length);
      case 5:
        return _deserialize5(
            buffer: buffer, offset: offset, messageLength: data.length);
      case 12:
        return _deserialize12(
            buffer: buffer, offset: offset, messageLength: data.length);
      default:
        return null;
    }
  }

  MetadataResponse? _deserialize0(
      {required ByteData buffer,
      required int offset,
      required int messageLength}) {
    final int throttleTimeMs = 0;
    final List<Broker> brokers = [];
    final List<KafkaTopicMetadata> topics = [];

    // Deserialize brokers
    final int brokersLength = buffer.getInt32(offset);
    offset += 4;

    for (int i = 0; i < brokersLength; i++) {
      final int nodeId = buffer.getInt32(offset);
      offset += 4;

      final int hostLength = buffer.getInt16(offset);
      offset += 2;

      final String host =
          String.fromCharCodes(buffer.buffer.asUint8List(offset, hostLength));
      offset += hostLength;

      final int port = buffer.getInt32(offset);
      offset += 4;

      brokers.add(Broker(nodeId: nodeId, host: host, port: port));
    }

    // Deserialize topics
    final int topicsLength = buffer.getInt32(offset);
    offset += 4;

    for (int i = 0; i < topicsLength; i++) {
      final int errorCode = buffer.getInt16(offset);
      offset += 2;

      final int topicNameLength = buffer.getInt16(offset);
      offset += 2;

      final String topicName = String.fromCharCodes(
          buffer.buffer.asUint8List(offset, topicNameLength));
      offset += topicNameLength;

      final bool isInternal = buffer.getInt8(offset) == 1;
      offset += 1;

      final List<KafkaPartitionMetadata> partitions = [];
      final int partitionsLength = buffer.getInt32(offset);
      offset += 4;

      for (int j = 0; j < partitionsLength; j++) {
        final int partitionErrorCode = buffer.getInt16(offset);
        offset += 2;

        final int partitionId = buffer.getInt32(offset);
        offset += 4;

        final int leaderId = buffer.getInt32(offset);
        offset += 4;

        final int replicasLength = buffer.getInt32(offset);
        offset += 4;

        final List<int> replicas = [];
        for (int k = 0; k < replicasLength; k++) {
          replicas.add(buffer.getInt32(offset));
          offset += 4;
        }

        final int isrLength = buffer.getInt32(offset);
        offset += 4;

        final List<int> isr = [];
        for (int k = 0; k < isrLength; k++) {
          isr.add(buffer.getInt32(offset));
          offset += 4;
        }

        partitions.add(KafkaPartitionMetadata(
          errorCode: partitionErrorCode,
          partitionId: partitionId,
          leaderId: leaderId,
          replicas: replicas,
          isr: isr,
        ));
      }

      topics.add(KafkaTopicMetadata(
        errorCode: errorCode,
        topicName: topicName,
        isInternal: isInternal,
        partitions: partitions,
      ));
    }

    return MetadataResponse(
      throttleTimeMs: throttleTimeMs,
      brokers: brokers,
      topics: topics,
    );
  }

  MetadataResponse? _deserialize1({
    required ByteData buffer,
    required int offset,
    required int messageLength,
  }) {
    final int throttleTimeMs = 0; // Not present in version 1
    final List<Broker> brokers = [];
    final List<KafkaTopicMetadata> topics = [];

    // Deserialize brokers
    final int brokersLength = buffer.getInt32(offset);
    offset += 4;

    if (brokersLength < 0) {
      throw Exception("Invalid brokersLength: $brokersLength");
    }

    for (int i = 0; i < brokersLength; i++) {
      final int nodeId = buffer.getInt32(offset);
      offset += 4;

      final int hostLength = buffer.getInt16(offset);
      offset += 2;

      if (offset + hostLength > buffer.lengthInBytes) {
        throw Exception("Insufficient bytes to read host");
      }

      final String host =
          String.fromCharCodes(buffer.buffer.asUint8List(offset, hostLength));
      offset += hostLength;

      final int port = buffer.getInt32(offset);
      offset += 4;

      brokers.add(Broker(nodeId: nodeId, host: host, port: port));
    }

    // Deserialize controller_id
    final int controllerId = buffer.getInt32(offset);
    offset += 4;

    // Deserialize topics
    final int topicsLength = buffer.getInt32(offset);
    offset += 4;

    if (topicsLength < 0) {
      throw Exception("Invalid topicsLength: $topicsLength");
    }

    for (int i = 0; i < topicsLength; i++) {
      final int errorCode = buffer.getInt16(offset);
      offset += 2;

      final int topicNameLength = buffer.getInt16(offset);
      offset += 2;

      if (offset + topicNameLength > buffer.lengthInBytes) {
        throw Exception("Insufficient bytes to read topicName");
      }

      final String topicName = String.fromCharCodes(
          buffer.buffer.asUint8List(offset, topicNameLength));
      offset += topicNameLength;

      final bool isInternal = buffer.getInt8(offset) == 1;
      offset += 1;

      final List<KafkaPartitionMetadata> partitions = [];
      final int partitionsLength = buffer.getInt32(offset);
      offset += 4;

      if (partitionsLength < 0) {
        throw Exception("Invalid partitionsLength: $partitionsLength");
      }

      for (int j = 0; j < partitionsLength; j++) {
        final int partitionErrorCode = buffer.getInt16(offset);
        offset += 2;

        final int partitionId = buffer.getInt32(offset);
        offset += 4;

        final int leaderId = buffer.getInt32(offset);
        offset += 4;

        final int replicasLength = buffer.getInt32(offset);
        offset += 4;

        if (replicasLength < 0) {
          throw Exception("Invalid replicasLength: $replicasLength");
        }

        final List<int> replicas = [];
        for (int k = 0; k < replicasLength; k++) {
          replicas.add(buffer.getInt32(offset));
          offset += 4;
        }

        final int isrLength = buffer.getInt32(offset);
        offset += 4;

        if (isrLength < 0) {
          throw Exception("Invalid isrLength: $isrLength");
        }

        final List<int> isr = [];
        for (int k = 0; k < isrLength; k++) {
          isr.add(buffer.getInt32(offset));
          offset += 4;
        }

        partitions.add(KafkaPartitionMetadata(
          errorCode: partitionErrorCode,
          partitionId: partitionId,
          leaderId: leaderId,
          replicas: replicas,
          isr: isr,
        ));
      }

      topics.add(KafkaTopicMetadata(
        errorCode: errorCode,
        topicName: topicName,
        isInternal: isInternal,
        partitions: partitions,
      ));
    }

    return MetadataResponse(
      throttleTimeMs: throttleTimeMs,
      brokers: brokers,
      topics: topics,
      controllerId: controllerId,
    );
  }

  MetadataResponse? _deserialize2({
    required ByteData buffer,
    required int offset,
    required int messageLength,
  }) {
    final List<Broker> brokers = [];
    final List<KafkaTopicMetadata> topics = [];

    final int brokersLength = buffer.getInt32(offset);
    offset += 4;

    if (brokersLength < 0) {
      throw Exception("Invalid brokersLength: $brokersLength");
    }

    for (int i = 0; i < brokersLength; i++) {
      final int nodeId = buffer.getInt32(offset);
      offset += 4;

      final int hostLength = buffer.getInt16(offset);
      offset += 2;

      if (offset + hostLength > buffer.lengthInBytes) {
        throw Exception("Insufficient bytes to read host");
      }

      final String host =
          String.fromCharCodes(buffer.buffer.asUint8List(offset, hostLength));
      offset += hostLength;

      final int port = buffer.getInt32(offset);
      offset += 4;

      // Deserialize rack (nullable string, new in version 1)
      final int rackLength = buffer.getInt16(offset);
      offset += 2;

      String? rack;
      if (rackLength != -1) {
        if (offset + rackLength > buffer.lengthInBytes) {
          throw Exception("Insufficient bytes to read rack");
        }
        rack =
            String.fromCharCodes(buffer.buffer.asUint8List(offset, rackLength));
        offset += rackLength;
      }

      brokers.add(Broker(nodeId: nodeId, host: host, port: port, rack: rack));
    }

    final int controllerId = buffer.getInt32(offset);
    offset += 4;

    final int topicsLength = buffer.getInt32(offset);
    offset += 4;

    if (topicsLength < 0) {
      throw Exception("Invalid topicsLength: $topicsLength");
    }

    for (int i = 0; i < topicsLength; i++) {
      final int errorCode = buffer.getInt16(offset);
      offset += 2;

      final int topicNameLength = buffer.getInt16(offset);
      offset += 2;

      if (offset + topicNameLength > buffer.lengthInBytes) {
        throw Exception("Insufficient bytes to read topicName");
      }

      final String topicName = String.fromCharCodes(
          buffer.buffer.asUint8List(offset, topicNameLength));
      offset += topicNameLength;

      final bool isInternal = buffer.getInt8(offset) == 1;
      offset += 1;

      final List<KafkaPartitionMetadata> partitions = [];
      final int partitionsLength = buffer.getInt32(offset);
      offset += 4;

      if (partitionsLength < 0) {
        throw Exception("Invalid partitionsLength: $partitionsLength");
      }

      for (int j = 0; j < partitionsLength; j++) {
        final int partitionErrorCode = buffer.getInt16(offset);
        offset += 2;

        final int partitionId = buffer.getInt32(offset);
        offset += 4;

        final int leaderId = buffer.getInt32(offset);
        offset += 4;

        final int replicasLength = buffer.getInt32(offset);
        offset += 4;

        if (replicasLength < 0) {
          throw Exception("Invalid replicasLength: $replicasLength");
        }

        final List<int> replicas = [];
        for (int k = 0; k < replicasLength; k++) {
          replicas.add(buffer.getInt32(offset));
          offset += 4;
        }

        final int isrLength = buffer.getInt32(offset);
        offset += 4;

        if (isrLength < 0) {
          throw Exception("Invalid isrLength: $isrLength");
        }

        final List<int> isr = [];
        for (int k = 0; k < isrLength; k++) {
          isr.add(buffer.getInt32(offset));
          offset += 4;
        }

        partitions.add(KafkaPartitionMetadata(
          errorCode: partitionErrorCode,
          partitionId: partitionId,
          leaderId: leaderId,
          replicas: replicas,
          isr: isr,
        ));
      }

      topics.add(KafkaTopicMetadata(
        errorCode: errorCode,
        topicName: topicName,
        isInternal: isInternal,
        partitions: partitions,
      ));
    }

    final int throttleTimeMs = buffer.getInt32(offset);
    offset += 4;

    return MetadataResponse(
      throttleTimeMs: throttleTimeMs,
      brokers: brokers,
      topics: topics,
      controllerId: controllerId,
    );
  }

  MetadataResponse? _deserialize3({
    required ByteData buffer,
    required int offset,
    required int messageLength,
  }) {
    final List<Broker> brokers = [];
    final List<KafkaTopicMetadata> topics = [];

    final int brokersLength = buffer.getInt32(offset);
    offset += 4;

    if (brokersLength < 0) {
      throw Exception("Invalid brokersLength: $brokersLength");
    }

    for (int i = 0; i < brokersLength; i++) {
      final int nodeId = buffer.getInt32(offset);
      offset += 4;

      final int hostLength = buffer.getInt16(offset);
      offset += 2;

      if (offset + hostLength > buffer.lengthInBytes) {
        throw Exception("Insufficient bytes to read host");
      }

      final String host =
          String.fromCharCodes(buffer.buffer.asUint8List(offset, hostLength));
      offset += hostLength;

      final int port = buffer.getInt32(offset);
      offset += 4;

      final int rackLength = buffer.getInt16(offset);
      offset += 2;

      String? rack;
      if (rackLength != -1) {
        if (offset + rackLength > buffer.lengthInBytes) {
          throw Exception("Insufficient bytes to read rack");
        }
        rack =
            String.fromCharCodes(buffer.buffer.asUint8List(offset, rackLength));
        offset += rackLength;
      }

      brokers.add(Broker(nodeId: nodeId, host: host, port: port, rack: rack));
    }

    final int clusterIdLength = buffer.getInt16(offset);
    offset += 2;

    String? clusterId;
    if (clusterIdLength != -1) {
      if (offset + clusterIdLength > buffer.lengthInBytes) {
        throw Exception("Insufficient bytes to read clusterId");
      }
      clusterId = String.fromCharCodes(
          buffer.buffer.asUint8List(offset, clusterIdLength));
      offset += clusterIdLength;
    }

    final int controllerId = buffer.getInt32(offset);
    offset += 4;

    final int topicsLength = buffer.getInt32(offset);
    offset += 4;

    if (topicsLength < 0) {
      throw Exception("Invalid topicsLength: $topicsLength");
    }

    for (int i = 0; i < topicsLength; i++) {
      final int errorCode = buffer.getInt16(offset);
      offset += 2;

      final int topicNameLength = buffer.getInt16(offset);
      offset += 2;

      if (offset + topicNameLength > buffer.lengthInBytes) {
        throw Exception("Insufficient bytes to read topicName");
      }

      final String topicName = String.fromCharCodes(
          buffer.buffer.asUint8List(offset, topicNameLength));
      offset += topicNameLength;

      final bool isInternal = buffer.getInt8(offset) == 1;
      offset += 1;

      final List<KafkaPartitionMetadata> partitions = [];
      final int partitionsLength = buffer.getInt32(offset);
      offset += 4;

      if (partitionsLength < 0) {
        throw Exception("Invalid partitionsLength: $partitionsLength");
      }

      for (int j = 0; j < partitionsLength; j++) {
        final int partitionErrorCode = buffer.getInt16(offset);
        offset += 2;

        final int partitionId = buffer.getInt32(offset);
        offset += 4;

        final int leaderId = buffer.getInt32(offset);
        offset += 4;

        final int replicasLength = buffer.getInt32(offset);
        offset += 4;

        if (replicasLength < 0) {
          throw Exception("Invalid replicasLength: $replicasLength");
        }

        final List<int> replicas = [];
        for (int k = 0; k < replicasLength; k++) {
          replicas.add(buffer.getInt32(offset));
          offset += 4;
        }

        final int isrLength = buffer.getInt32(offset);
        offset += 4;

        if (isrLength < 0) {
          throw Exception("Invalid isrLength: $isrLength");
        }

        final List<int> isr = [];
        for (int k = 0; k < isrLength; k++) {
          isr.add(buffer.getInt32(offset));
          offset += 4;
        }

        partitions.add(KafkaPartitionMetadata(
          errorCode: partitionErrorCode,
          partitionId: partitionId,
          leaderId: leaderId,
          replicas: replicas,
          isr: isr,
        ));
      }

      topics.add(KafkaTopicMetadata(
        errorCode: errorCode,
        topicName: topicName,
        isInternal: isInternal,
        partitions: partitions,
      ));
    }

    final int throttleTimeMs = buffer.getInt32(offset);
    offset += 4;

    final int clusterAuthorizedOperations = buffer.getInt32(offset);
    offset += 4;

    return MetadataResponse(
      throttleTimeMs: throttleTimeMs,
      brokers: brokers,
      topics: topics,
      controllerId: controllerId,
      clusterId: clusterId,
      clusterAuthorizedOperations: clusterAuthorizedOperations,
    );
  }

  MetadataResponse? _deserialize5({
    required ByteData buffer,
    required int offset,
    required int messageLength,
  }) {
    final List<Broker> brokers = [];
    final List<KafkaTopicMetadata> topics = [];

    final int brokersLength = buffer.getInt32(offset);
    offset += 4;

    if (brokersLength < 0) {
      throw Exception("Invalid brokersLength: $brokersLength");
    }

    for (int i = 0; i < brokersLength; i++) {
      final int nodeId = buffer.getInt32(offset);
      offset += 4;

      final int hostLength = buffer.getInt16(offset);
      offset += 2;

      if (offset + hostLength > buffer.lengthInBytes) {
        throw Exception("Insufficient bytes to read host");
      }

      final String host =
          String.fromCharCodes(buffer.buffer.asUint8List(offset, hostLength));
      offset += hostLength;

      final int port = buffer.getInt32(offset);
      offset += 4;

      // Deserialize rack (nullable string, introduced in version 1)
      final int rackLength = buffer.getInt16(offset);
      offset += 2;

      String? rack;
      if (rackLength != -1) {
        if (offset + rackLength > buffer.lengthInBytes) {
          throw Exception("Insufficient bytes to read rack");
        }
        rack =
            String.fromCharCodes(buffer.buffer.asUint8List(offset, rackLength));
        offset += rackLength;
      }

      brokers.add(Broker(nodeId: nodeId, host: host, port: port, rack: rack));
    }

    final int clusterIdLength = buffer.getInt16(offset);
    offset += 2;

    String? clusterId;
    if (clusterIdLength != -1) {
      if (offset + clusterIdLength > buffer.lengthInBytes) {
        throw Exception("Insufficient bytes to read clusterId");
      }
      clusterId = String.fromCharCodes(
          buffer.buffer.asUint8List(offset, clusterIdLength));
      offset += clusterIdLength;
    }

    final int controllerId = buffer.getInt32(offset);
    offset += 4;

    final int topicsLength = buffer.getInt32(offset);
    offset += 4;

    if (topicsLength < 0) {
      throw Exception("Invalid topicsLength: $topicsLength");
    }

    for (int i = 0; i < topicsLength; i++) {
      final int errorCode = buffer.getInt16(offset);
      offset += 2;

      final int topicNameLength = buffer.getInt16(offset);
      offset += 2;

      if (offset + topicNameLength > buffer.lengthInBytes) {
        throw Exception("Insufficient bytes to read topicName");
      }

      final String topicName = String.fromCharCodes(
          buffer.buffer.asUint8List(offset, topicNameLength));
      offset += topicNameLength;

      final bool isInternal = buffer.getInt8(offset) == 1;
      offset += 1;

      final List<KafkaPartitionMetadata> partitions = [];
      final int partitionsLength = buffer.getInt32(offset);
      offset += 4;

      if (partitionsLength < 0) {
        throw Exception("Invalid partitionsLength: $partitionsLength");
      }

      for (int j = 0; j < partitionsLength; j++) {
        final int partitionErrorCode = buffer.getInt16(offset);
        offset += 2;

        final int partitionId = buffer.getInt32(offset);
        offset += 4;

        final int leaderId = buffer.getInt32(offset);
        offset += 4;

        final int replicasLength = buffer.getInt32(offset);
        offset += 4;

        if (replicasLength < 0) {
          throw Exception("Invalid replicasLength: $replicasLength");
        }

        final List<int> replicas = [];
        for (int k = 0; k < replicasLength; k++) {
          replicas.add(buffer.getInt32(offset));
          offset += 4;
        }

        final int isrLength = buffer.getInt32(offset);
        offset += 4;

        if (isrLength < 0) {
          throw Exception("Invalid isrLength: $isrLength");
        }

        final List<int> isr = [];
        for (int k = 0; k < isrLength; k++) {
          isr.add(buffer.getInt32(offset));
          offset += 4;
        }

        partitions.add(KafkaPartitionMetadata(
          errorCode: partitionErrorCode,
          partitionId: partitionId,
          leaderId: leaderId,
          replicas: replicas,
          isr: isr,
        ));
      }

      final int topicAuthorizedOperations = buffer.getInt32(offset);
      offset += 4;

      topics.add(KafkaTopicMetadata(
        errorCode: errorCode,
        topicName: topicName,
        isInternal: isInternal,
        partitions: partitions,
        topicAuthorizedOperations: topicAuthorizedOperations,
      ));
    }

    final int throttleTimeMs = buffer.getInt32(offset);
    offset += 4;

    final int clusterAuthorizedOperations = buffer.getInt32(offset);
    offset += 4;

    return MetadataResponse(
      throttleTimeMs: throttleTimeMs,
      brokers: brokers,
      topics: topics,
      controllerId: controllerId,
      clusterId: clusterId,
      clusterAuthorizedOperations: clusterAuthorizedOperations,
    );
  }

  MetadataResponse? _deserialize12({
    required ByteData buffer,
    required int offset,
    required int messageLength,
  }) {
    final List<Broker> brokers = [];
    final List<KafkaTopicMetadata> topics = [];

    final int throttleTimeMs = buffer.getInt32(offset);
    offset += 4;

    final int brokersLength = buffer.getInt16(offset);
    offset += 2;

    if (brokersLength < 0) {
      throw Exception("Invalid brokersLength: $brokersLength");
    }

    for (int i = 0; i < brokersLength; i++) {
      final int nodeId = buffer.getInt32(offset);
      offset += 4;

      final int hostLength = buffer.getInt16(offset);
      offset += 2;

      if (offset + hostLength > buffer.lengthInBytes) {
        throw Exception("Insufficient bytes to read host");
      }

      final String host =
          String.fromCharCodes(buffer.buffer.asUint8List(offset, hostLength));
      offset += hostLength;

      final int port = buffer.getInt32(offset);
      offset += 4;

      final int rackLength = buffer.getInt16(offset);
      offset += 2;

      String? rack;
      if (rackLength != -1) {
        if (offset + rackLength > buffer.lengthInBytes) {
          throw Exception("Insufficient bytes to read rack");
        }
        rack =
            String.fromCharCodes(buffer.buffer.asUint8List(offset, rackLength));
        offset += rackLength;
      }

      brokers.add(Broker(nodeId: nodeId, host: host, port: port, rack: rack));
    }

    final int clusterIdLength = buffer.getInt16(offset);
    offset += 2;

    String? clusterId;
    if (clusterIdLength != -1) {
      if (offset + clusterIdLength > buffer.lengthInBytes) {
        throw Exception("Insufficient bytes to read clusterId");
      }
      clusterId = String.fromCharCodes(
          buffer.buffer.asUint8List(offset, clusterIdLength));
      offset += clusterIdLength;
    }

    final int controllerId = buffer.getInt32(offset);
    offset += 4;

    final int topicsLength = buffer.getInt32(offset);
    offset += 4;

    if (topicsLength < 0) {
      throw Exception("Invalid topicsLength: $topicsLength");
    }

    for (int i = 0; i < topicsLength; i++) {
      final int errorCode = buffer.getInt16(offset);
      offset += 2;

      final int topicNameLength = buffer.getInt16(offset);
      offset += 2;

      if (offset + topicNameLength > buffer.lengthInBytes) {
        throw Exception("Insufficient bytes to read topicName");
      }

      final String topicName = String.fromCharCodes(
          buffer.buffer.asUint8List(offset, topicNameLength));
      offset += topicNameLength;

      final bool isInternal = buffer.getInt8(offset) == 1;
      offset += 1;

      final List<KafkaPartitionMetadata> partitions = [];
      final int partitionsLength = buffer.getInt32(offset);
      offset += 4;

      if (partitionsLength < 0) {
        throw Exception("Invalid partitionsLength: $partitionsLength");
      }

      for (int j = 0; j < partitionsLength; j++) {
        final int partitionErrorCode = buffer.getInt16(offset);
        offset += 2;

        final int partitionId = buffer.getInt32(offset);
        offset += 4;

        final int leaderId = buffer.getInt32(offset);
        offset += 4;

        final int leaderEpoch = buffer.getInt32(offset);
        offset += 4;

        final int replicasLength = buffer.getInt32(offset);
        offset += 4;

        if (replicasLength < 0) {
          throw Exception("Invalid replicasLength: $replicasLength");
        }

        final List<int> replicas = [];
        for (int k = 0; k < replicasLength; k++) {
          replicas.add(buffer.getInt32(offset));
          offset += 4;
        }

        final int isrLength = buffer.getInt32(offset);
        offset += 4;

        if (isrLength < 0) {
          throw Exception("Invalid isrLength: $isrLength");
        }

        final List<int> isr = [];
        for (int k = 0; k < isrLength; k++) {
          isr.add(buffer.getInt32(offset));
          offset += 4;
        }

        partitions.add(KafkaPartitionMetadata(
          errorCode: partitionErrorCode,
          partitionId: partitionId,
          leaderId: leaderId,
          leaderEpoch: leaderEpoch,
          replicas: replicas,
          isr: isr,
        ));
      }

      // Deserialize topic_authorized_operations (introduced in version 5)
      final int topicAuthorizedOperations = buffer.getInt32(offset);
      offset += 4;

      topics.add(KafkaTopicMetadata(
        errorCode: errorCode,
        topicName: topicName,
        isInternal: isInternal,
        partitions: partitions,
        topicAuthorizedOperations: topicAuthorizedOperations,
      ));
    }

    return MetadataResponse(
      throttleTimeMs: throttleTimeMs,
      brokers: brokers,
      topics: topics,
      controllerId: controllerId,
      clusterId: clusterId,
    );
  }
}
