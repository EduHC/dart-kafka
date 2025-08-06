import 'dart:convert';
import 'dart:typed_data';

import '../models/components/record.dart';
import 'utils.dart';

// ignore: constant_identifier_names
const KAFKA_MAGIC = 2;

class Encoder {
  Utils utils = Utils();

  Uint8List int8(int value) {
    final data = ByteData(1)..setInt8(0, value);
    return data.buffer.asUint8List();
  }

  Uint8List uint8(int value) {
    final ByteData data = ByteData(1)..setUint8(0, value);
    return data.buffer.asUint8List();
  }

  Uint8List int16(int value) {
    final data = ByteData(2)..setInt16(0, value);
    return data.buffer.asUint8List();
  }

  Uint8List uint16(int value) {
    final ByteData data = ByteData(2)..setUint16(0, value);
    return data.buffer.asUint8List();
  }

  Uint8List int32(int value) {
    final data = ByteData(4)..setInt32(0, value);
    return data.buffer.asUint8List();
  }

  Uint8List uint32(int value) {
    final data = ByteData(4)..setUint32(0, value);
    return data.buffer.asUint8List();
  }

  Uint8List int64(int value) {
    final data = ByteData(8)..setInt64(0, value);
    return data.buffer.asUint8List();
  }

  Uint8List uint64(int value) {
    final ByteData data = ByteData(8)..setUint64(0, value);
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

  Uint8List compactArrayLength(int length) =>
      encodeUnsignedVarint(length <= 0 ? 0 : length + 1);

  Uint8List tagBuffer() => int8(0);

  Uint8List encodeUnsignedVarint(int value) {
    final bytes = <int>[];
    while (value > 0x7F) {
      bytes.add((value & 0x7F) | 0x80);
      value >>= 7;
    }
    bytes.add(value);
    return Uint8List.fromList(bytes);
  }

  Uint8List writeVarint(int value) =>
      writeUnsignedVarint((value << 1) ^ (value >> 31));

  Uint8List writeUnsignedVarint(int value) {
    final BytesBuilder buffer = BytesBuilder();

    if ((value & (0xFFFFFFFF << 7)) == 0) {
      buffer.add(uint8(value));
    } else {
      buffer.add(uint8(value & 0x7F | 0x80));
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
            buffer
              ..add(uint8((value >>> 21) & 0x7F | 0x80))
              ..add(uint8((value >>> 28) & 0xFF));
          }
        }
      }
    }

    return buffer.toBytes();
  }

  Uint8List writeVarlong(int value) =>
      writeUnsignedVarlong((value << 1) ^ (value >> 63));

  Uint8List writeUnsignedVarlong(int value) {
    final BytesBuilder buffer = BytesBuilder();
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

  Uint8List writeMessageHeader({
    required int version,
    required int messageLength,
    required int apiKey,
    required int apiVersion,
    required int correlationId,
    required String? clientId,
  }) {
    final BytesBuilder buffer = BytesBuilder()
      ..add(int16(apiKey))
      ..add(int16(apiVersion))
      ..add(int32(correlationId));

    if (version > 0) {
      buffer.add(nullableString(clientId));
    }
    if (version > 1) {
      buffer.add(int8(0)); // _tagged_fields
    }

    return Uint8List.fromList(
      [...int32(messageLength + buffer.length), ...buffer.toBytes()],
    );
  }

  Uint8List writeRecordBatch({
    required List<Record> records,
    required int producerId,
    required int attributes,
    required int lastOffsetDelta,
    required int producerEpoch,
    required int baseSequence,
    required int batchOffset,
    required int partitionLeaderEpoch,
  }) {
    final Uint8List baseBatch = _writeRecordBatchInnerBody(
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
      partitionLeaderEpoch: partitionLeaderEpoch,
    );
  }

  Uint8List _writeRecordBatchInnerBody({
    required List<Record> records,
    required int producerId,
    required int attributes,
    required int lastOffsetDelta,
    required int producerEpoch,
    required int baseSequence,
  }) {
    final BytesBuilder batchBuffer = BytesBuilder()
      ..add(int16(attributes)) // attributes
      ..add(int32(lastOffsetDelta)) // lastOffsetDelta
      ..add(int64(records.first.timestamp)) // BaseTimestamp
      ..add(int64(records.last.timestamp)) // maxTimestamp
      ..add(int64(producerId)) // ProducerId
      ..add(int16(producerEpoch)) // ProducerEpoch
      ..add(int32(baseSequence)) // BaseSequence
      ..add(int32(records.length));

    for (int i = 0; i < records.length; i++) {
      batchBuffer.add(
        _writeRecord(
          records[i],
          i,
          records[i].timestamp - records.first.timestamp,
        ),
      );
    }

    return batchBuffer.toBytes();
  }

  Uint8List _writeRecord(Record record, int index, int timestampDelta) {
    final BytesBuilder recordBuffer = BytesBuilder()
      ..addByte(0) // Attributes
      ..add(writeVarlong(timestampDelta))
      ..add(writeVarint(index)); // OffsetDelta

    if (record.key == null) {
      recordBuffer.add(writeVarint(-1));
    } else {
      final Uint8List keyBytes = utf8.encode(record.key!);
      recordBuffer
        ..add(writeVarint(keyBytes.length))
        ..add(keyBytes);
    }

    if (record.value == null) {
      recordBuffer.add(writeVarint(-1));
    } else {
      final Uint8List valueBytes = utf8.encode(record.value!);
      recordBuffer
        ..add(writeVarint(valueBytes.length))
        ..add(valueBytes);
    }

    recordBuffer.add(writeVarint(record.headers?.length ?? 0));
    for (final header in record.headers ?? []) {
      if (header.key == null) {
        throw Exception('Invalid null header key found!');
      }
      Uint8List bytes = utf8.encode(header.key!);
      recordBuffer
        ..add(writeVarint(bytes.length))
        ..add(bytes);

      if (header.value == null) {
        recordBuffer.add(writeVarint(-1));
      } else {
        bytes = utf8.encode(header.value);
        recordBuffer
          ..add(writeVarint(bytes.length))
          ..add(bytes);
      }
    }

    final int length = utils.recordSizeOfBodyInBytes(
      headers: record.headers ?? [],
      offsetDelta: index,
      timestampDelta: timestampDelta,
      key: record.key == null ? null : utf8.encode(record.key!),
      value: record.value == null ? null : utf8.encode(record.value!),
    );

    // BytesBuilder varintLength = utils.writeVarint(length, BytesBuilder());
    return Uint8List.fromList(
      [...writeVarint(length), ...recordBuffer.toBytes()],
    );
  }

  Uint8List _appendBatchHeader({
    required Uint8List base,
    required int batchOffset,
    required int partitionLeaderEpoch,
  }) {
    final Uint8List message = _appendBatchMiddleHeader(
      base: base,
      partitionLeaderEpoch: partitionLeaderEpoch,
    );

    return Uint8List.fromList(
      [...int64(batchOffset), ...int32(message.length), ...message],
    );
  }

  Uint8List _appendBatchMiddleHeader({
    required Uint8List base,
    required int partitionLeaderEpoch,
  }) =>
      Uint8List.fromList([
        ...int32(partitionLeaderEpoch),
        ...int8(KAFKA_MAGIC),
        ...uint32(utils.crc32c(base)),
        ...base,
      ]);

  Uint8List bytes(Uint8List value) {
    final length = int32(value.length);
    return Uint8List.fromList([...length, ...value]);
  }

  Uint8List boolean({required bool value}) => int8(value ? 1 : 0);
}
