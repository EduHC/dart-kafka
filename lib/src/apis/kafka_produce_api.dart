import 'dart:typed_data';

import 'package:dart_kafka/src/models/components/produce_response_component.dart';
import 'package:dart_kafka/src/models/components/record_error.dart';
import 'package:dart_kafka/src/models/partition.dart';
import 'package:dart_kafka/src/models/responses/produce_response.dart';
import 'package:dart_kafka/src/models/topic.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/protocol/decoder.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/definitions/errors.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaProduceApi {
  final int apiKey = PRODUCE; // ProduceRequest API key
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Serialize the ProduceRequest
  Uint8List serialize({
    required int correlationId,
    String? transactionalId,
    required int acks,
    required int timeoutMs,
    required List<Topic> topics,
    int apiVersion = 11,
    String? clientId,
    required int producerId,
    required int attributes,
    required int lastOffsetDelta,
    required int producerEpoch,
    required int baseSequence,
    required int batchOffset,
    required int partitionLeaderEpoch,
  }) {
    BytesBuilder byteBuffer = BytesBuilder();

    if (apiVersion >= 9) {
      byteBuffer.add(encoder.compactNullableString(transactionalId));
    } else {
      byteBuffer.add(encoder.nullableString(transactionalId));
    }

    byteBuffer.add(encoder.int16(acks));
    byteBuffer.add(encoder.int32(timeoutMs));

    if (apiVersion >= 9) {
      byteBuffer.add(encoder.compactArrayLength(topics.length));
    } else {
      byteBuffer.add(encoder.int32(topics.length));
    }

    for (final topic in topics) {
      if (apiVersion >= 9) {
        byteBuffer.add(encoder.compactString(topic.topicName));
        byteBuffer
            .add(encoder.compactArrayLength(topic.partitions?.length ?? 0));
      } else {
        byteBuffer.add(encoder.string(topic.topicName));
        byteBuffer.add(encoder.int32(topic.partitions?.length ?? 0));
      }

      for (Partition partition in topic.partitions ?? []) {
        if (partition.batch == null) {
          throw Exception("The partition ${partition.id} has a null batch.");
        }

        if (partition.batch!.records == null) {
          throw Exception(
              "The batch of the partition ${partition.id} has null records");
        }

        byteBuffer.add(encoder.int32(partition.id));
        final Uint8List recordBatch = encoder.writeRecordBatch(
          records: partition.batch!.records!,
          producerId: producerId,
          attributes: attributes,
          lastOffsetDelta: lastOffsetDelta,
          producerEpoch: producerEpoch,
          baseSequence: baseSequence,
          batchOffset: batchOffset,
          partitionLeaderEpoch: partitionLeaderEpoch,
        );

        // byteBuffer = encoder.writeUnsignedVarint(recordBatch.length + 1); // adding the size of the RecordBatch
        byteBuffer.add(encoder.writeUnsignedVarint(recordBatch.length + 1));
        byteBuffer.add(recordBatch);
      }
    }

    if (apiVersion >= 9) {
      // Adding the _tagged_fields
      byteBuffer.addByte(0);
      byteBuffer.addByte(0);
      byteBuffer.addByte(0);
    }

    final message = byteBuffer.toBytes();
    byteBuffer.clear();

    return Uint8List.fromList([
      ...encoder.writeMessageHeader(
        apiKey: apiKey,
        apiVersion: apiVersion,
        clientId: clientId,
        correlationId: correlationId,
        messageLength: message.length,
        version: apiVersion > 8 ? 2 : 1,
      ),
      ...message
    ]);
  }

  /// Deserialize the ProduceResponse
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    // Read responses array
    final result = decoder.readCompactArrayLength(buffer, offset);
    final responsesLength = result.value;
    offset += result.bytesRead;

    final responses = <ProduceResponseComponent>[];
    for (int i = 0; i < responsesLength; i++) {
      final topic = decoder.readCompactString(buffer, offset);
      final String topicName = topic.value;
      offset += topic.bytesRead;

      // Read partition_responses array
      final partitions = decoder.readCompactArrayLength(buffer, offset);
      final partitionResponsesLength = partitions.value;
      offset += partitions.bytesRead;

      final partitionResponses = <Partition>[];
      for (int j = 0; j < partitionResponsesLength; j++) {
        final partition = buffer.getInt32(offset);
        offset += 4;
        final errorCode = buffer.getInt16(offset);
        offset += 2;
        final baseOffset = buffer.getInt64(offset);
        offset += 8;
        final logAppendTime = buffer.getInt64(offset);
        offset += 8;
        final logStartOffset = buffer.getInt64(offset);
        offset += 8;

        final errors = decoder.readCompactArrayLength(buffer, offset);
        offset += errors.bytesRead;

        final errorResponses = <RecordError>[];
        for (int k = 0; k < errors.value; k++) {
          final int batchIndex = buffer.getInt32(offset);
          final batchErrorIndex =
              decoder.readCompactNullableString(buffer, offset);
          offset += batchErrorIndex.bytesRead;
          final int taggedFields = buffer.getInt8(offset);

          errorResponses.add(RecordError(
              batchIndex: batchIndex, errorMessage: batchErrorIndex.value));
        }

        final errorMessage = decoder.readCompactNullableString(buffer, offset);
        offset += errorMessage.bytesRead;
        final int taggedFields1 = buffer.getInt8(offset);
        offset += 1;
        final int taggedFields2 = buffer.getInt8(offset);
        offset += 1;
        final int throttleTimeMs = buffer.getInt32(offset);
        offset += 4;
        final int taggedFields3 = buffer.getInt8(offset);
        offset += 1;

        partitionResponses.add(
          Partition(
              id: partition,
              errorCode: errorCode,
              errorMessage: errorMessage.value ??
                  (ERROR_MAP[errorCode] as Map)['message'],
              baseOffset: baseOffset,
              logAppendTimeMs: logAppendTime,
              logStartOffset: logStartOffset,
              recordErrors: errorResponses),
        );
      }

      responses.add(ProduceResponseComponent(
        topicName: topicName,
        partitions: partitionResponses,
      ));
    }

    var throttleTimeMs;

    if (utils.canRead(currentOffset: offset, amountOfBytes: 4, data: data)) {
      throttleTimeMs = buffer.getInt32(offset);
      offset += 4;
    }

    // Read TAG_BUFFER (empty for now)
    decoder.readTagBuffer(buffer, offset);

    return ProduceResponse(
      responses: responses,
      throttleTimeMs: throttleTimeMs,
    );
  }
}
