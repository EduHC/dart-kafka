import 'dart:typed_data';

import '../definitions/apis.dart';
import '../definitions/errors.dart';
import '../models/components/active_producer.dart';
import '../models/partition.dart';
import '../models/responses/describe_producer_response.dart';
import '../models/topic.dart';
import '../protocol/decoder.dart';
import '../protocol/endocer.dart';
import '../protocol/utils.dart';

class KafkaDescribeProducerApi {
  final int apiKey = DESCRIBE_PRODUCERS;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Serialize the DescribeProducerRequest
  Uint8List serialize({
    required int correlationId,
    required List<Topic> topics,
    int apiVersion = 11,
    String? clientId,
  }) {
    final BytesBuilder byteBuffer = BytesBuilder();

    if (topics.isEmpty) {
      throw Exception('No topics informed for DescribeProducer');
    }

    byteBuffer.add(encoder.compactArrayLength(topics.length));
    for (int i = 0; i < topics.length; i++) {
      byteBuffer
        ..add(encoder.compactString(topics[i].topicName))
        ..add(encoder.compactArrayLength(topics[i].partitions?.length ?? 0));
      for (int j = 0; j < (topics[i].partitions?.length ?? 0); j++) {
        byteBuffer.add(encoder.int32(topics[i].partitions![j].id));
      }
      // add _tagged_field
      byteBuffer.add(encoder.int8(0));
    }

    // add _tagged_field
    byteBuffer.add(encoder.int8(0));
    final message = byteBuffer.toBytes();
    byteBuffer.clear();

    return Uint8List.fromList([
      ...encoder.writeMessageHeader(
        version: 2,
        messageLength: message.length,
        apiKey: apiKey,
        apiVersion: apiVersion,
        correlationId: correlationId,
        clientId: clientId,
      ),
      ...message,
    ]);
  }

  /// Method to deserialize the DescribeProducerResponse from a Byte Array
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    final int throttleTime = buffer.getInt32(offset);
    offset += 4;

    final List<Topic> topics = [];
    final topicsLength = decoder.readCompactArrayLength(buffer, offset);
    offset += topicsLength.bytesRead;

    for (int i = 0; i < topicsLength.value; i++) {
      final topicName = decoder.readCompactString(buffer, offset);
      offset += topicName.bytesRead;

      final partitionsLength = decoder.readCompactArrayLength(buffer, offset);
      offset += partitionsLength.bytesRead;

      final List<Partition> partitions = [];
      for (int j = 0; j < partitionsLength.value; j++) {
        final int partitionId = buffer.getInt32(offset);
        offset += 4;

        final int errorCode = buffer.getInt16(offset);
        offset += 2;

        final errorMessage = decoder.readCompactNullableString(buffer, offset);
        offset += errorMessage.bytesRead;

        final activeProducerLength =
            decoder.readCompactArrayLength(buffer, offset);
        offset += activeProducerLength.bytesRead;

        final List<ActiveProducer> activeProducers = [];
        for (int k = 0; k < activeProducerLength.value; k++) {
          final int producerId = buffer.getInt64(offset);
          offset += 8;

          final int producerEpoch = buffer.getInt32(offset);
          offset += 4;

          final int lastSequence = buffer.getInt32(offset);
          offset += 4;

          final int lastTimestamp = buffer.getInt64(offset);
          offset += 8;

          final int coordinatorEpoch = buffer.getInt32(offset);
          offset += 4;

          final int currentTxnStartOffset = buffer.getInt64(offset);
          offset += 8;

          // final int activePRoducerTaggedField = buffer.getInt8(offset);
          offset += 1;

          activeProducers.add(
            ActiveProducer(
              id: producerId,
              epoch: producerEpoch,
              lastSequence: lastSequence,
              lastTimestamp: lastTimestamp,
              coordinatorEpoch: coordinatorEpoch,
              currentTxnStartOffset: currentTxnStartOffset,
            ),
          );
        }

        // final int partitionTaggedField = buffer.getInt8(offset);
        offset += 1;

        partitions.add(
          Partition(
            id: partitionId,
            activeProducers: activeProducers,
            errorCode: errorCode,
            errorMessage:
                errorMessage.value ?? (ERROR_MAP[errorCode]! as Map)['message'],
          ),
        );
      }

      topics.add(Topic(topicName: topicName.value, partitions: partitions));
    }

    // final int baseTaggedField = buffer.getInt8(offset);
    offset += 1;

    return DescribeProducerResponse(throttleTime: throttleTime, topics: topics);
  }
}
