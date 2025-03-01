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
}
