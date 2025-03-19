import 'dart:convert';
import 'dart:typed_data';

import 'package:dart_kafka/src/protocol/utils.dart';
import 'package:dart_kafka/src/models/components/record.dart';

// ignore: constant_identifier_names
const KAFKA_MAGIC = 2;

class Encoder {
  Utils utils = Utils();

  Uint8List int8(int value) {
    final data = ByteData(1);
    data.setInt8(0, value);
    return data.buffer.asUint8List();
  }

  Uint8List uint8(int value) {
    ByteData data = ByteData(1);
    data.setUint8(0, value);
    return data.buffer.asUint8List();
  }

  Uint8List int16(int value) {
    final data = ByteData(2);
    data.setInt16(0, value, Endian.big);
    return data.buffer.asUint8List();
  }

  Uint8List uint16(int value) {
    ByteData data = ByteData(2);
    data.setUint16(0, value);
    return data.buffer.asUint8List();
  }

  Uint8List int32(int value) {
    final data = ByteData(4);
    data.setInt32(0, value, Endian.big);
    return data.buffer.asUint8List();
  }

  Uint8List uint32(int value) {
    final data = ByteData(4);
    data.setUint32(0, value, Endian.big);
    return data.buffer.asUint8List();
  }

  Uint8List int64(int value) {
    final data = ByteData(8);
    data.setInt64(0, value, Endian.big);
    return data.buffer.asUint8List();
  }

  Uint8List uint64(int value) {
    ByteData data = ByteData(8);
    data.setUint64(0, value);
    return data.buffer.asUint8List();
  }

  Uint8List string(String value) {
    final bytes = utf8.encode(value);
    return Uint8List.fromList([...int16(bytes.length), ...bytes]);
  }

  Uint8List nullableString(String? value) {
    if (value == null) {
      return Uint8List.fromList([255, 255]);
    }
    return string(value);
  }

  Uint8List compactString(String value) {
    final bytes = utf8.encode(value);
    final length = encodeUnsignedVarint(bytes.length + 1);
    return Uint8List.fromList([...length, ...bytes]);
  }

  Uint8List compactNullableString(String? value) {
    if (value == null) {
      return Uint8List.fromList([0]);
    }
    return compactString(value);
  }

  Uint8List compactBytes(Uint8List value) {
    final length = encodeUnsignedVarint(value.length + 1);
    return Uint8List.fromList([...length, ...value]);
  }

  Uint8List compactArrayLength(int length) {
    return encodeUnsignedVarint(length <= 0 ? 0 : length + 1);
  }

  Uint8List tagBuffer() {
    return Uint8List(0);
  }

  Uint8List encodeUnsignedVarint(int value) {
    final bytes = <int>[];
    while (value > 0x7F) {
      bytes.add((value & 0x7F) | 0x80);
      value >>= 7;
    }
    bytes.add(value);
    return Uint8List.fromList(bytes);
  }

  Uint8List writeVarint(int value) {
    return writeUnsignedVarint((value << 1) ^ (value >> 31));
  }

  Uint8List writeUnsignedVarint(int value) {
    BytesBuilder buffer = BytesBuilder();

    if ((value & (0xFFFFFFFF << 7)) == 0) {
      buffer.add(uint8(value));
    } else {
      buffer.add((uint8(value & 0x7F | 0x80)));
      if ((value & (0xFFFFFFFF << 14)) == 0) {
        buffer.add(uint8((value >>> 7) & 0xFF));
      } else {
        buffer.add(uint8((value >>> 7) & 0x7F | 0x80));
        if ((value & (0xFFFFFFFF << 21)) == 0) {
          buffer.add(uint8((value >>> 14) & 0xFF));
        } else {
          buffer.add(uint8((value >>> 14) & 0x7F | 0x80));
          if ((value & (0xFFFFFFFF << 28)) == 0) {
            buffer.add(uint8((value >>> 21) & 0xFF));
          } else {
            buffer.add(uint8((value >>> 21) & 0x7F | 0x80));
            buffer.add(uint8((value >>> 28) & 0xFF));
          }
        }
      }
    }

    return buffer.toBytes();
  }

  Uint8List writeVarlong(int value) {
    return writeUnsignedVarlong((value << 1) ^ (value >> 63));
  }

  Uint8List writeUnsignedVarlong(int value) {
    BytesBuilder buffer = BytesBuilder();
    ByteData? data = ByteData(1);
    int? offset = 0;

    while ((value & 0xffffffffffffff80) != 0) {
      final byte = (value & 0x7f) | 0x80;
      data.setUint8(offset!, byte);
      offset++;
      value >>>= 7;
    }
    data.setInt8(offset!, value);
    buffer.add(data.buffer.asUint8List());

    offset = null;
    data = null;
    return buffer.toBytes();
  }

  Uint8List writeMessageHeader(
      {required int version,
      required int messageLength,
      required int apiKey,
      required int apiVersion,
      required int correlationId,
      required String? clientId}) {
    BytesBuilder buffer = BytesBuilder();
    buffer.add(int16(apiKey));
    buffer.add(int16(apiVersion));
    buffer.add(int32(correlationId));

    if (version > 0) {
      buffer.add(nullableString(clientId));
    }
    if (version > 1) {
      buffer.add(int8(0)); // _tagged_fields
    }

    return Uint8List.fromList(
        [...int32(messageLength + buffer.length), ...buffer.toBytes()]);
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

    batchBuffer.add(int16(attributes)); // attributes
    batchBuffer.add(int32(lastOffsetDelta)); // lastOffsetDelta
    batchBuffer.add(int64(records.first.timestamp)); // BaseTimestamp
    batchBuffer.add(int64(records.last.timestamp)); // maxTimestamp
    batchBuffer.add(int64(producerId)); // ProducerId
    batchBuffer.add(int16(producerEpoch)); // ProducerEpoch
    batchBuffer.add(int32(baseSequence)); // BaseSequence
    batchBuffer.add(int32(records.length));

    for (int i = 0; i < records.length; i++) {
      batchBuffer.add(_writeRecord(
          records[i], i, records[i].timestamp - records.first.timestamp));
    }

    return batchBuffer.toBytes();
  }

  Uint8List _writeRecord(Record record, int index, int timestampDelta) {
    BytesBuilder recordBuffer = BytesBuilder();

    recordBuffer.addByte(0); // Attributes
    recordBuffer.add(writeVarlong(timestampDelta));
    // recordBuffer = writeVarlong(timestampDelta, recordBuffer); // timestampDelta
    recordBuffer.add(writeVarint(index)); // OffsetDelta
    // recordBuffer = utils.writeVarint(index, recordBuffer); // OffsetDelta

    if (record.key == null) {
      // recordBuffer = utils.writeVarint(-1, recordBuffer);
      recordBuffer.add(writeVarint(-1));
    } else {
      final Uint8List keyBytes = utf8.encode(record.key!);
      recordBuffer.add(writeVarint(keyBytes.length));
      // recordBuffer = utils.writeVarint(keyBytes.length, recordBuffer);
      recordBuffer.add(keyBytes);
    }

    if (record.value == null) {
      recordBuffer.add(writeVarint(-1));
      // recordBuffer = utils.writeVarint(-1, recordBuffer);
    } else {
      final Uint8List valueBytes = utf8.encode(record.value!);
      // recordBuffer = utils.writeVarint(valueBytes.length, recordBuffer);
      recordBuffer.add(writeVarint(valueBytes.length));
      recordBuffer.add(valueBytes);
    }

    // recordBuffer = utils.writeVarint(record.headers?.length ?? 0, recordBuffer);
    recordBuffer.add(writeVarint(record.headers?.length ?? 0));
    for (var header in record.headers ?? []) {
      if (header.key == null) {
        throw Exception("Invalid null header key found!");
      }
      Uint8List bytes = utf8.encode(header.key!);
      // recordBuffer = utils.writeVarint(bytes.length, recordBuffer);
      recordBuffer.add(writeVarint(bytes.length));
      recordBuffer.add(bytes);

      if (header.value == null) {
        // recordBuffer = utils.writeVarint(-1, recordBuffer);
        recordBuffer.add(writeVarint(-1));
      } else {
        bytes = utf8.encode(header.value);
        // recordBuffer = utils.writeVarint(bytes.length, recordBuffer);
        recordBuffer.add(writeVarint(bytes.length));
        recordBuffer.add(bytes);
      }
    }

    int length = utils.recordSizeOfBodyInBytes(
        headers: record.headers ?? [],
        offsetDelta: index,
        timestampDelta: timestampDelta,
        key: record.key == null ? null : utf8.encode(record.key!),
        value: record.value == null ? null : utf8.encode(record.value!));

    // BytesBuilder varintLength = utils.writeVarint(length, BytesBuilder());
    return Uint8List.fromList(
        [...writeVarint(length), ...recordBuffer.toBytes()]);
  }

  Uint8List _appendBatchHeader(
      {required Uint8List base,
      required int batchOffset,
      required int partitionLeaderEpoch}) {
    Uint8List message = _appendBatchMiddleHeader(
        base: base, partitionLeaderEpoch: partitionLeaderEpoch);

    return Uint8List.fromList(
        [...int64(batchOffset), ...int32(message.length), ...message]);
  }

  Uint8List _appendBatchMiddleHeader(
      {required Uint8List base, required int partitionLeaderEpoch}) {
    return Uint8List.fromList([
      ...int32(partitionLeaderEpoch),
      ...int8(KAFKA_MAGIC),
      ...uint32(utils.crc32c(base)),
      ...base
    ]);
  }
}
