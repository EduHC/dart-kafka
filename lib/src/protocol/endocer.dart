import 'dart:convert';
import 'dart:typed_data';

import 'package:dart_kafka/src/protocol/utils.dart';
import 'package:dart_kafka/src/models/components/record.dart';

// ignore: constant_identifier_names
const KAFKA_MAGIC = 2;

class Encoder {
  Utils utils = Utils();

  Uint8List writeMessageHeader(
      {required int version,
      required int messageLength,
      required int apiKey,
      required int apiVersion,
      required int correlationId,
      required String? clientId}) {
    BytesBuilder buffer = BytesBuilder();
    buffer.add(utils.int16(apiKey));
    buffer.add(utils.int16(apiVersion));
    buffer.add(utils.int32(correlationId));

    if (version > 0) {
      buffer.add(utils.nullableString(clientId));
    }
    if (version > 1) {
      buffer.add(utils.int8(0)); // _tagged_fields
    }

    return Uint8List.fromList(
        [...utils.int32(messageLength + buffer.length), ...buffer.toBytes()]);
  }

  Uint8List writeRecordBatch(
      {required List<Record> records,
      required int producerId,
      required int attributes,
      required int lastOffsetDelta,
      required int producerEpoch,
      required int baseSequence,
      required int batchOffset,
      required int partitionLeaderEpoch}) {
    Uint8List baseBatch = _writeRecordBatchInnerBody(
      records: records,
      attributes: attributes,
      baseSequence: baseSequence,
      lastOffsetDelta: lastOffsetDelta,
      producerEpoch: producerEpoch,
      producerId: producerId,
    );

    return _appendBatchHeader(
        base: baseBatch,
        batchOffset: batchOffset,
        partitionLeaderEpoch: partitionLeaderEpoch);
  }

  Uint8List _writeRecordBatchInnerBody(
      {required List<Record> records,
      required int producerId,
      required int attributes,
      required int lastOffsetDelta,
      required int producerEpoch,
      required int baseSequence}) {
    BytesBuilder batchBuffer = BytesBuilder();

    batchBuffer.add(utils.int16(attributes)); // attributes
    batchBuffer.add(utils.int32(lastOffsetDelta)); // lastOffsetDelta
    batchBuffer.add(utils.int64(records.first.timestamp)); // BaseTimestamp
    batchBuffer.add(utils.int64(records.last.timestamp)); // maxTimestamp
    batchBuffer.add(utils.int64(producerId)); // ProducerId
    batchBuffer.add(utils.int16(producerEpoch)); // ProducerEpoch
    batchBuffer.add(utils.int32(baseSequence)); // BaseSequence
    batchBuffer.add(utils.int32(records.length));

    for (int i = 0; i < records.length; i++) {
      batchBuffer.add(_writeRecord(
          records[i], i, records[i].timestamp - records.first.timestamp));
    }

    return batchBuffer.toBytes();
  }

  Uint8List _writeRecord(Record record, int index, int timestampDelta) {
    BytesBuilder recordBuffer = BytesBuilder();

    recordBuffer.addByte(0); // Attributes
    recordBuffer =
        utils.writeVarlong(timestampDelta, recordBuffer); // timestampDelta
    recordBuffer = utils.writeVarint(index, recordBuffer); // OffsetDelta

    if (record.key == null) {
      recordBuffer = utils.writeVarint(-1, recordBuffer);
    } else {
      final Uint8List keyBytes = utf8.encode(record.key!);
      recordBuffer = utils.writeVarint(keyBytes.length, recordBuffer);
      recordBuffer.add(keyBytes);
    }

    if (record.value == null) {
      recordBuffer = utils.writeVarint(-1, recordBuffer);
    } else {
      final Uint8List valueBytes = utf8.encode(record.value!);
      recordBuffer = utils.writeVarint(valueBytes.length, recordBuffer);
      recordBuffer.add(valueBytes);
    }

    recordBuffer = utils.writeVarint(record.headers?.length ?? 0, recordBuffer);
    for (var header in record.headers ?? []) {
      if (header.key == null) {
        throw Exception("Invalid null header key found!");
      }
      Uint8List bytes = utf8.encode(header.key!);
      recordBuffer = utils.writeVarint(bytes.length, recordBuffer);
      recordBuffer.add(bytes);

      if (header.value == null) {
        recordBuffer = utils.writeVarint(-1, recordBuffer);
      } else {
        bytes = utf8.encode(header.value);
        recordBuffer = utils.writeVarint(bytes.length, recordBuffer);
        recordBuffer.add(bytes);
      }
    }

    int length = utils.recordSizeOfBodyInBytes(
        headers: record.headers ?? [],
        offsetDelta: index,
        timestampDelta: timestampDelta,
        key: record.key == null ? null : utf8.encode(record.key!),
        value: record.value == null ? null : utf8.encode(record.value!));

    BytesBuilder varintLength = utils.writeVarint(length, BytesBuilder());

    return Uint8List.fromList(
        [...varintLength.toBytes(), ...recordBuffer.toBytes()]);
  }

  Uint8List _appendBatchHeader(
      {required Uint8List base,
      required int batchOffset,
      required int partitionLeaderEpoch}) {
    Uint8List message = _appendBatchMiddleHeader(
        base: base, partitionLeaderEpoch: partitionLeaderEpoch);

    return Uint8List.fromList([
      ...utils.int64(batchOffset),
      ...utils.int32(message.length),
      ...message
    ]);
  }

  Uint8List _appendBatchMiddleHeader(
      {required Uint8List base, required int partitionLeaderEpoch}) {
    return Uint8List.fromList([
      ...utils.int32(partitionLeaderEpoch),
      ...utils.int8(KAFKA_MAGIC),
      ...utils.uint32(utils.crc32c(base)),
      ...base
    ]);
  }
}
