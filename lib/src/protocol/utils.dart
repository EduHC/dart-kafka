import 'dart:math';
import 'dart:typed_data';
import 'package:dart_kafka/src/models/components/record_header.dart';
import 'package:dart_kafka/src/protocol/crc32.dart';

class Utils {
  int crc32c(Uint8List data) {
    return CRC32C.calculate(data);
  }

  int generateCorrelationId() {
    final Random random = Random();
    int value = random.nextInt(1 << 32);
    return value - (1 << 31); // Convert to signed int32 range
  }

  bool canRead(
      {required int currentOffset,
      required int amountOfBytes,
      required List<int> data}) {
    return data.length >= (currentOffset + amountOfBytes);
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
      if (header.key == null) {
        throw Exception("Invalid null header key found in headers");
      }

      int headerKeySize = utf8Length(header.key);
      size += sizeOfVarint(headerKeySize) + headerKeySize;

      if (header.value == null) {
        size += sizeOfVarint(-1);
      } else {
        int headerValueSize = utf8Length(header.value);
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
