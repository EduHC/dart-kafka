import 'dart:typed_data';

import 'package:dart_kafka/src/models/kafka_broker.dart';
import 'package:dart_kafka/src/models/kafka_partition_metadata.dart';
import 'package:dart_kafka/src/models/kafka_topic_metadata.dart';
import 'package:dart_kafka/src/protocol/utils.dart';
import 'package:dart_kafka/src/models/metadata_response.dart';
import 'package:dart_kafka/src/protocol/apis.dart';

class KafkaMetadataApi {
  final int apiKey;
  final Utils utils = Utils();

  KafkaMetadataApi() : apiKey = METADATA;

  /// Method to serialize to build and serialize the MetadataRequest to Byte Array
  Uint8List serialize(
      {required int correlationId,
      String? clientId,
      required List<String> topics,
      required int apiVersion}) {
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

    // Add additional fields for version 5 (e.g., allowAutoTopicCreation)
    if (apiVersion >= 5) {
      byteBuffer.add(utils.int8(1)); // allowAutoTopicCreation = true
    }

    Uint8List message = byteBuffer.toBytes();
    return Uint8List.fromList([...utils.int32(message.length), ...message]);
  }

  /// Method to deserialize the MetadataResponse from a Byte Array
  MetadataResponse? deserialize({required Uint8List data}) {
    if (data.length < 14) {
      print("Invalid byte array: Insufficient length for MetadataResponse");
      return null;
    }

    final buffer = ByteData.sublistView(data);
    int offset = 0;

    final int messageLength = buffer.getInt32(offset);
    offset += 4;

    final int correlationId = buffer.getInt32(offset);
    offset += 4;

    final int apiKey = buffer.getInt16(offset);
    offset += 2;

    final int apiVersion = buffer.getInt16(offset);
    offset += 2;

    Map<String, int> header = {
      'length': messageLength,
      'correlationId': correlationId,
      'apiKey': apiKey,
      'apiVersion': apiVersion
    };
    print("Received: $header");

    switch (apiVersion) {
      case 0:
        return _deserialize0(
            buffer: buffer, offset: offset, messageLength: messageLength);
      case 1:
        return _deserialize1(
            buffer: buffer, offset: offset, messageLength: messageLength);
      case 2:
        return _deserialize2(
            buffer: buffer, offset: offset, messageLength: messageLength);
      case 3:
        return _deserialize3(
            buffer: buffer, offset: offset, messageLength: messageLength);
      case 4:
        return _deserialize4(
            buffer: buffer, offset: offset, messageLength: messageLength);
      case 5:
        return _deserialize5(
            buffer: buffer, offset: offset, messageLength: messageLength);
      default:
        return null;
    }
  }

  MetadataResponse? _deserialize0(
      {required ByteData buffer,
      required int offset,
      required int messageLength}) {
    final int throttleTimeMs = 0; // Not present in version 0
    final List<KafkaBroker> brokers = [];
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

      brokers.add(KafkaBroker(nodeId: nodeId, host: host, port: port));
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

  MetadataResponse? _deserialize1(
      {required ByteData buffer,
      required int offset,
      required int messageLength}) {
    // Deserialize MetadataResponse version 1
    // Similar to version 0 but with additional fields
    // Implement according to the protocol specification
    return null;
  }

  MetadataResponse? _deserialize2(
      {required ByteData buffer,
      required int offset,
      required int messageLength}) {
    // Deserialize MetadataResponse version 2
    // Similar to version 1 but with additional fields
    // Implement according to the protocol specification
    return null;
  }

  MetadataResponse? _deserialize3(
      {required ByteData buffer,
      required int offset,
      required int messageLength}) {
    // Deserialize MetadataResponse version 3
    // Similar to version 2 but with additional fields
    // Implement according to the protocol specification
    return null;
  }

  MetadataResponse? _deserialize4(
      {required ByteData buffer,
      required int offset,
      required int messageLength}) {
    // Deserialize MetadataResponse version 4
    // Similar to version 3 but with additional fields
    // Implement according to the protocol specification
    return null;
  }

  MetadataResponse? _deserialize5(
      {required ByteData buffer,
      required int offset,
      required int messageLength}) {
    // Deserialize MetadataResponse version 5
    // Similar to version 4 but with additional fields
    // Implement according to the protocol specification
    return null;
  }
}
