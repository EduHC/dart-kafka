import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';
import 'package:dart_kafka/src/models/components/record.dart';
import 'package:dart_kafka/src/models/components/record_header.dart';
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

  // Uint8List varint(int value) {
  //   final buffer = BytesBuilder();
  //   while ((value & ~0x7F) != 0) {
  //     buffer.addByte((value & 0x7F) | 0x80);
  //     value >>= 7;
  //   }
  //   buffer.addByte(value);
  //   return buffer.toBytes();
  // }

  BytesBuilder writeVarint(int value, BytesBuilder buffer) {
    return writeUnsignedVarint((value << 1) ^ (value >> 31), buffer);
    // return writeUnsignedVarint(value, buffer);
  }

  BytesBuilder writeUnsignedVarint(int value, BytesBuilder buffer) {
    if ((value & (0xFFFFFFFF << 7)) == 0) {
      buffer.add(uint8(value));
    } else {
      buffer.add((uint8((value & 0x7F) | 0x80)));
      if ((value & (0xFFFFFFFF << 14)) == 0) {
        buffer.add(uint8(value >>> 7));
      } else {
        buffer.add(uint8((value >>> 7) & 0x7F | 0x80));
        if ((value & (0xFFFFFFFF << 21)) == 0) {
          buffer.add(uint8(value >>> 14));
        } else {
          buffer.add(uint8((value >>> 14) & 0x7F | 0x80));
          if ((value & (0xFFFFFFFF << 28)) == 0) {
            buffer.add(uint8(value >>> 21));
          } else {
            buffer.add(uint8((value >>> 21) & 0x7F | 0x80));
            buffer.add(uint8(value >>> 28));
          }
        }
      }
    }

    return buffer;
  }

  BytesBuilder writeVarlong(int value, BytesBuilder buffer) {
    return writeUnsignedVarlong((value << 1) ^ (value >> 63), buffer);
  }

  BytesBuilder writeUnsignedVarlong(int value, BytesBuilder buffer) {
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
    return buffer;
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

  Uint8List recordBatch(List<Record> records) {
    BytesBuilder batchBuffer = BytesBuilder();

    batchBuffer.add(int16(0)); // attributes
    batchBuffer.add(int32(0)); // lastOffsetDelta
    batchBuffer.add(int64(records.first.timestamp)); // BaseTimestamp
    batchBuffer.add(int64(records.last.timestamp)); // maxTimestamp
    batchBuffer.add(int64(1000)); // ProducerId ||
    batchBuffer.add(int16(0)); // ProducerEpoch
    batchBuffer.add(int32(0)); // BaseSequence
    batchBuffer.add(int32(records.length));

    // Encode each record
    for (var record in records) {
      BytesBuilder recordBuffer = BytesBuilder();

      recordBuffer.addByte(0); // Attributes
      recordBuffer = writeVarlong(record.timestamp - records.first.timestamp,
          recordBuffer); // timestampDelta
      recordBuffer =
          writeVarint(records.indexOf(record), recordBuffer); // OffsetDelta

      if (record.key == null) {
        recordBuffer = writeVarint(-1, recordBuffer);
      } else {
        final Uint8List keyBytes = utf8.encode(record.key!);
        recordBuffer = writeVarint(keyBytes.length, recordBuffer);
        recordBuffer.add(keyBytes);
      }

      if (record.value == null) {
        recordBuffer = writeVarint(-1, recordBuffer);
      } else {
        final Uint8List valueBytes = utf8.encode(record.value!);
        recordBuffer = writeVarint(valueBytes.length, recordBuffer);
        recordBuffer.add(valueBytes);
      }

      recordBuffer = writeVarint(record.headers?.length ?? 0, recordBuffer);
      for (var header in record.headers ?? []) {
        if (header.key == null) {
          throw Exception("Invalid null header key found!");
        }
        Uint8List bytes = utf8.encode(header.key!);
        recordBuffer = writeVarint(bytes.length, recordBuffer);
        recordBuffer.add(bytes);

        if (header.value == null) {
          recordBuffer = writeVarint(-1, recordBuffer);
        } else {
          bytes = utf8.encode(header.value);
          recordBuffer = writeVarint(bytes.length, recordBuffer);
          recordBuffer.add(bytes);
        }
      }

      // add the Varint length of the record
      batchBuffer = writeVarint(
          recordSizeOfBodyInBytes(
              headers: record.headers ?? [],
              offsetDelta: 0,
              timestampDelta: 0,
              key: record.key == null ? null : utf8.encode(record.key!),
              value: record.value == null ? null : utf8.encode(record.value!)),
          batchBuffer);

      batchBuffer.add([...recordBuffer.toBytes()]);
    }

    Uint8List message = batchBuffer.toBytes();

    // Add the PartitionLeaderEpoch, magic, Checksum
    message = Uint8List.fromList([
      ...int32(-1),
      ...int8(2),
      ...uint32(crc32c(message)),
      ...message,
    ]);
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

  int recordSizeOfBodyInBytes(
      {required int offsetDelta,
      required int timestampDelta,
      Uint8List? key,
      Uint8List? value,
      required List<RecordHeader> headers}) {
    int keySize = key == null ? -1 : key.length;
    int valueSize = value == null ? -1 : value.length;

    return recordSizeOfBodyInBytes0(
        offsetDelta, timestampDelta, keySize, valueSize, headers);
  }

  int recordSizeOfBodyInBytes0(int offsetDelta, int timestampDelta, int keySize,
      int valueSize, List<RecordHeader> headers) {
    int size = 1; // 1 for the Attributes
    size += sizeOfVarint(offsetDelta);
    size += sizeOfVarlong(timestampDelta);
    size += _sizeOfRecordPart(
        keySize: keySize, valueSize: valueSize, headers: headers);
    return size;
  }

  int sizeOfVarint(int value) {
    return sizeOfUnsignedVarint((value << 1) ^ (value >> 31));
  }

  int sizeOfUnsignedVarint(int value) {
    int leadingZeros = (value == 0) ? 32 : (32 - value.bitLength);

    // 74899 is equal to the byte-value 0b10010010010010011 in Java -- used by Apache Kafka.
    int leadingZerosBelow38DividedBy7 = ((38 - leadingZeros) * 74899) >>> 19;
    return leadingZerosBelow38DividedBy7 + (leadingZeros >>> 5);
  }

  int sizeOfVarlong(int value) {
    return sizeOfUnsignedVarlong((value << 1) ^ (value >> 63));
  }

  int sizeOfUnsignedVarlong(int value) {
    int leadingZeros = (value == 0) ? 64 : (64 - value.bitLength);

    // 74899 is equal to the byte-value 0b10010010010010011 in Java -- used by Apache Kafka.
    int leadingZerosBelow38DividedBy7 = ((70 - leadingZeros) * 74899) >>> 19;
    return leadingZerosBelow38DividedBy7 + (leadingZeros >>> 6);
  }

  int _sizeOfRecordPart(
      {required int keySize,
      required int valueSize,
      required List<RecordHeader> headers}) {
    int size = 0;

    if (keySize < 0) {
      size += sizeOfVarint(-1);
    } else {
      size += sizeOfVarint(keySize) + keySize;
    }

    if (valueSize < 0) {
      size += sizeOfVarint(-1);
    } else {
      size += sizeOfVarint(valueSize) + valueSize;
    }

    size += sizeOfVarint(headers.length);
    for (RecordHeader header in headers) {
      if (header.headerKey == null) {
        throw Exception("Invalid null header key found in headers");
      }

      int headerKeySize = utf8Length(header.headerKey);
      size += sizeOfVarint(headerKeySize) + headerKeySize;

      if (header.headerValue == null) {
        size += sizeOfVarint(-1);
      } else {
        int headerValueSize = utf8Length(header.headerValue);
        size += sizeOfVarint(headerValueSize) + headerValueSize;
      }
    }
    return size;
  }

  int utf8Length(String s) {
    int count = 0;

    for (final int rune in s.runes) {
      if (rune <= 0x7F) {
        count++;
      } else if (rune <= 0x7FF) {
        count += 2;
      } else if (rune <= 0xFFFF) {
        count += 3;
      } else {
        // Code points above 0xFFFF are encoded as four bytes in UTF-8.
        count += 4;
      }
    }
    return count;
  }
}
