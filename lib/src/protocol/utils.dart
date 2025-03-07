import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';
import 'package:dart_kafka/src/models/components/record.dart';
import 'package:dart_kafka/src/protocol/crc32.dart';

class Utils {
  int crc32c(Uint8List data) {
    return CRC32C.calculate(data);
  }

  Uint8List int8(int value) {
    final data = ByteData(1);
    data.setInt8(0, value);
    return data.buffer.asUint8List();
  }

  Uint8List int16(int value) {
    final data = ByteData(2);
    data.setInt16(0, value, Endian.big);
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

  Uint8List varInt(int value) {
    final buffer = BytesBuilder();
    while ((value & ~0x7F) != 0) {
      buffer.addByte((value & 0x7F) | 0x80);
      value >>= 7;
    }
    buffer.addByte(value);
    return buffer.toBytes();
  }

  Uint8List varLong(int value) {
    final buffer = BytesBuilder();
    while ((value & ~0x7F) != 0) {
      buffer.addByte((value & 0x7F) | 0x80);
      value >>>= 7; // Use unsigned right shift
    }
    buffer.addByte(value);
    return buffer.toBytes();
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
    return encodeUnsignedVarint(length < 0 ? 0 : length + 1);
  }

  Uint8List tagBuffer() {
    return Uint8List(0);
  }

  Uint8List recordBatch(List<Record> records) {
    BytesBuilder batchBuffer = BytesBuilder();

    batchBuffer.add(int16(0)); // attributes
    batchBuffer.add(int32(0)); // lastOffsetDelta
    batchBuffer.add(int64(records.first.timestamp)); // BaseTimestamp
    batchBuffer.add(int64(records.last.timestamp)); // maxTimestamp
    batchBuffer.add(int64(0)); // ProducerId || mandando como 0
    batchBuffer.add(int16(0)); // ProducerEpoch
    batchBuffer.add(int32(0)); // BaseSequence
    batchBuffer.add(compactArrayLength(records.length));

    // Encode each record
    for (var record in records) {
      BytesBuilder recordBuffer = BytesBuilder();

      recordBuffer.addByte(0); // Attributes
      recordBuffer.add(varLong(record.timestamp - records.first.timestamp)); // timestampDelta
      recordBuffer.add(varInt(records.indexOf(record))); // offsetDelta

      if (record.key != null) {
        recordBuffer.add(varInt(record.key!.length));
        recordBuffer.add(utf8.encode(record.key!)); // testar colocar ut8.encode
      } else {
        recordBuffer.add(varInt(0));
      }

      if (record.value != null) {
        recordBuffer.add(varInt(record.value!.length));
        recordBuffer.add(utf8.encode(record.value!)); // testar colocar ut8.encode
      } else {
        recordBuffer.add(varInt(0));
      }

      recordBuffer.add(compactArrayLength(record.headers?.length ?? 0));
      for (var header in record.headers ?? []) {
        recordBuffer.add(varInt(header.headerKey.length));
        recordBuffer.add(header.headerKey.codeUnits);
        recordBuffer.add(varInt(header.headerValue.length));
        recordBuffer.add(header.headerValue.codeUnits);
      }

      batchBuffer
          .add([...varInt(recordBuffer.length), ...recordBuffer.toBytes()]);
    }

    Uint8List message = batchBuffer.toBytes();

    // Add the PartitionLeaderEpoch, magic, Checksum and 3 more null tagged_fields
    message = Uint8List.fromList(
        [...int32(0), ...int8(2), ...uint32(crc32c(message)), ...message]);
    batchBuffer.clear();

    // Add the BatchOffset and BatchLength
    return Uint8List.fromList(
        [...int64(0), ...int32(message.length), ...message]);
  }

  /// Generate a random CorrelationID
  int generateCorrelationId() {
    final Random random = Random();
    int value = random.nextInt(1 << 32);
    return value - (1 << 31); // Convert to signed int32 range
  }

  ({int value, int bytesRead}) readVarint(List<int> byteArray, int offset,
      {bool signed = true}) {
    int value = 0;
    int shift = 0;
    int pos = offset;
    int bytesRead = 0;

    // print("Reading VarInt at offset: $offset");
    while (true) {
      if (pos >= byteArray.length) {
        throw Exception("Unexpected end of data while reading VarInt.");
      }

      int byte = byteArray[pos];
      // print("Byte read: ${byte.toRadixString(16)} at position $pos");
      pos++;
      bytesRead++;

      value |= (byte & 0x7F) << shift;
      shift += 7;

      if ((byte & 0x80) == 0) break;
    }

    // Apply zigzag decoding if signed is true
    if (signed) {
      value = (value >>> 1) ^ -(value & 1);
    }

    // print("Decoded VarInt: $value, Bytes Read: $bytesRead\n");
    return (value: value, bytesRead: bytesRead);
  }

  ({int value, int bytesRead}) readVarlong(List<int> byteArray, int offset,
      {bool signed = false}) {
    int value = 0;
    int shift = 0;
    int bytesRead = 0;
    int byte;

    do {
      if (offset + bytesRead >= byteArray.length) {
        throw Exception(
            "Invalid byte array: Insufficient bytes to read varlong");
      }

      if (shift > 63) {
        throw Exception("Invalid Long, must contain 9 bytes or less");
      }

      byte = byteArray[offset + bytesRead];
      value |= (byte & 0x7F) << shift;
      shift += 7;
      bytesRead++;
    } while ((byte & 0x80) != 0);

    // Apply zigzag decoding if signed is true
    if (signed) {
      value = (value >> 1) ^ -(value & 1);
    }

    return (value: value, bytesRead: bytesRead);
  }

  bool canRead(
      {required int currentOffset,
      required int amountOfBytes,
      required List<int> data}) {
    return data.length >= (currentOffset + amountOfBytes);
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

  ({String value, int bytesRead}) readCompactString(
      ByteData buffer, int offset) {
    final decoded = decodeUnsignedVarint(buffer, offset);
    final length = decoded.value;
    final string = String.fromCharCodes(
        buffer.buffer.asUint8List(offset + decoded.bytesRead, length - 1));
    return (value: string, bytesRead: decoded.bytesRead + length - 1);
  }

  ({int value, int bytesRead}) decodeUnsignedVarint(
      ByteData buffer, int offset) {
    int value = 0;
    int shift = 0;
    int bytesRead = 0;

    while (true) {
      final byte = buffer.getUint8(offset + bytesRead);
      value |= (byte & 0x7F) << shift;
      bytesRead++;

      if ((byte & 0x80) == 0) break;

      shift += 7;
    }

    return (value: value, bytesRead: bytesRead);
  }

  ({String? value, int bytesRead}) readCompactNullableString(
      ByteData buffer, int offset) {
    final decoded = decodeUnsignedVarint(buffer, offset);
    final length = decoded.value;
    if (length == 0) {
      return (value: null, bytesRead: decoded.bytesRead);
    }
    final string = String.fromCharCodes(
        buffer.buffer.asUint8List(offset + decoded.bytesRead, length - 1));
    return (value: string, bytesRead: decoded.bytesRead + length - 1);
  }

  ({Uint8List value, int bytesRead}) readCompactBytes(
      ByteData buffer, int offset) {
    final decoded = decodeUnsignedVarint(buffer, offset);
    final length = decoded.value;
    final value =
        buffer.buffer.asUint8List(offset + decoded.bytesRead, length - 1);
    return (value: value, bytesRead: decoded.bytesRead + length - 1);
  }

  ({int value, int bytesRead}) readCompactArrayLength(
      ByteData buffer, int offset) {
    final decoded = decodeUnsignedVarint(buffer, offset);
    return (value: decoded.value - 1, bytesRead: decoded.bytesRead);
  }

  int readTagBuffer(ByteData buffer, int offset) {
    return 0;
  }

  ({String value, int bytesRead}) readString(ByteData buffer, int offset) {
    final length = buffer.getInt16(offset);
    offset += 2;
    final value =
        String.fromCharCodes(buffer.buffer.asUint8List(offset, length));
    return (value: value, bytesRead: length + 2);
  }
}
