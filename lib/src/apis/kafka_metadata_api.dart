import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/definitions/errors.dart';
import 'package:dart_kafka/src/protocol/decoder.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/protocol/utils.dart';
import 'package:dart_kafka/src/definitions/apis.dart';

class KafkaMetadataApi {
  final int apiKey = METADATA;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

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
    final Encoder encoder = Encoder();

    if (apiVersion >= 9) {
      byteBuffer.add(encoder.compactArrayLength(topics.length));
    } else {
      byteBuffer.add(encoder.int32(topics.length));
    }

    for (var topic in topics) {
      if (apiVersion >= 10) {
        // topicId
        byteBuffer.add([
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
        ]);
      }
      if (apiVersion >= 9) {
        byteBuffer.add(encoder.compactNullableString(topic));
        byteBuffer.add(encoder.int8(0)); // _tagged_fields
      } else {
        byteBuffer.add(encoder.string(topic));
      }
    }

    if (apiVersion >= 4) {
      byteBuffer.add(encoder.int8(allowAutoTopicCreation ? 1 : 0));
    }

    if (apiVersion >= 8) {
      if (apiVersion < 11) {
        byteBuffer
            .add(encoder.int8(includeClusterAuthorizedOperations ? 1 : 0));
      }
      byteBuffer.add(encoder.int8(includeTopicAuthorizedOperations ? 1 : 0));
    }

    if (apiVersion >= 9) {
      // Add _tagged_fields
      byteBuffer.add(encoder.int8(0));
    }

    Uint8List message = byteBuffer.toBytes();
    return Uint8List.fromList([
      ...encoder.writeMessageHeader(
          version: apiVersion >= 9 ? 2 : 1,
          messageLength: message.length,
          apiKey: apiKey,
          apiVersion: apiVersion,
          correlationId: correlationId,
          clientId: clientId),
      ...message
    ]);
  }

  /// Method to deserialize the MetadataResponse from a Byte Array
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    final int throttleTimeMs = buffer.getInt32(offset);
    offset += 4;

    final brokersLength = decoder.readCompactArrayLength(buffer, offset);
    offset += brokersLength.bytesRead;

    List<Broker> brokers = [];
    for (int i = 0; i < brokersLength.value; i++) {
      final int nodeId = buffer.getInt32(offset);
      offset += 4;

      final host = decoder.readCompactString(buffer, offset);
      offset += host.bytesRead;

      final int port = buffer.getInt32(offset);
      offset += 4;

      final rack = decoder.readCompactNullableString(buffer, offset);
      offset += rack.bytesRead;

      final int bTaggedField = buffer.getInt8(offset);
      offset += 1;

      brokers.add(Broker(
          nodeId: nodeId, host: host.value, port: port, rack: rack.value));
    }

    final clusterId = decoder.readCompactNullableString(buffer, offset);
    offset += clusterId.bytesRead;

    final int controllerId = buffer.getInt32(offset);
    offset += 4;

    final topicsLength = decoder.readCompactArrayLength(buffer, offset);
    offset += topicsLength.bytesRead;

    List<KafkaTopicMetadata> topics = [];
    for (int i = 0; i < topicsLength.value; i++) {
      final int errorCode = buffer.getInt16(offset);
      offset += 2;

      final name = decoder.readCompactString(buffer, offset);
      offset += name.bytesRead;

      final bool isInternal = decoder.readBool(buffer, offset);
      offset += 1;

      final partitionsLenth = decoder.readCompactArrayLength(buffer, offset);
      offset += partitionsLenth.bytesRead;

      List<KafkaPartitionMetadata> partitions = [];
      for (int j = 0; j < partitionsLenth.value; j++) {
        final int pErrorCode = buffer.getInt16(offset);
        offset += 2;

        final int pId = buffer.getInt32(offset);
        offset += 4;

        final int leaderId = buffer.getInt32(offset);
        offset += 4;

        final int leaderEpoch = buffer.getInt32(offset);
        offset += 4;

        final replicaNodesLength =
            decoder.readCompactArrayLength(buffer, offset);
        offset += replicaNodesLength.bytesRead;

        List<int> replicaNodes = [];
        for (int k = 0; k < replicaNodesLength.value; k++) {
          final int replicaNodeId = buffer.getInt32(offset);
          offset += 4;
          replicaNodes.add(replicaNodeId);
        }

        final isrNodesLength = decoder.readCompactArrayLength(buffer, offset);
        offset += isrNodesLength.bytesRead;

        List<int> isrNodes = [];
        for (int k = 0; k < isrNodesLength.value; k++) {
          final int isrNodeId = buffer.getInt32(offset);
          offset += 4;
          isrNodes.add(isrNodeId);
        }

        final offlineReplicasLength =
            decoder.readCompactArrayLength(buffer, offset);
        offset += offlineReplicasLength.bytesRead;

        List<int> offlineReplicas = [];
        for (int k = 0; k < offlineReplicasLength.value; k++) {
          final int offlineReplicaId = buffer.getInt32(offset);
          offset += 4;
          offlineReplicas.add(offlineReplicaId);
        }

        final int pTaggedField = buffer.getInt8(offset);
        offset += 1;

        partitions.add(KafkaPartitionMetadata(
            errorCode: pErrorCode,
            errorMessage: (ERROR_MAP[pErrorCode] as Map)['message'],
            partitionId: pId,
            leaderId: leaderId,
            leaderEpoch: leaderEpoch,
            replicas: offlineReplicas,
            isr: isrNodes,
            offlineReplicas: offlineReplicas));
      }

      final int topicAuthorizedOperations = buffer.getInt32(offset);
      offset += 4;

      final int tTaggedField = buffer.getInt8(offset);
      offset += 1;

      topics.add(KafkaTopicMetadata(
          topicName: name.value,
          partitions: partitions,
          errorCode: errorCode,
          isInternal: isInternal,
          topicAuthorizedOperations: topicAuthorizedOperations));
    }

    final int clusterAuthorizedOperations = buffer.getInt32(offset);
    offset += 4;

    final int taggedField = buffer.getInt8(offset);
    offset += 1;

    return MetadataResponse(
      brokers: brokers,
      topics: topics,
      clusterId: clusterId.value,
      controllerId: controllerId,
      throttleTimeMs: throttleTimeMs,
      clusterAuthorizedOperations: clusterAuthorizedOperations,
    );
  }
}
