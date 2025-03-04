import 'dart:math';
import 'dart:typed_data';

class Utils {
  /// Helper to encode Int8
  Uint8List int8(int value) {
    final data = ByteData(1);
    data.setInt8(0, value);
    return data.buffer.asUint8List();
  }

  /// Helper to encode Int16 in Big Endian
  Uint8List int16(int value) {
    final data = ByteData(2);
    data.setInt16(0, value, Endian.big);
    return data.buffer.asUint8List();
  }

  /// Helper to encode Int32 in Big Endian
  Uint8List int32(int value) {
    final data = ByteData(4);
    data.setInt32(0, value, Endian.big);
    return data.buffer.asUint8List();
  }

  /// Helper to encode Int64 in Big Endian
  Uint8List int64(int value) {
    final data = ByteData(8);
    data.setInt64(0, value, Endian.big);
    return data.buffer.asUint8List();
  }

  /// Encode VarInt (Kafka uses variable-length encoding)
  Uint8List varInt(int value) {
    final buffer = BytesBuilder();
    while ((value & ~0x7F) != 0) {
      buffer.addByte((value & 0x7F) | 0x80);
      value >>= 7;
    }
    buffer.addByte(value);
    return buffer.toBytes();
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

    print("Reading VarInt at offset: $offset");
    while (true) {
      if (pos >= byteArray.length) {
        throw Exception("Unexpected end of data while reading VarInt.");
      }

      int byte = byteArray[pos];
      print("Byte read: ${byte.toRadixString(16)} at position $pos");
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

    print("Decoded VarInt: $value, Bytes Read: $bytesRead\n");
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
}
