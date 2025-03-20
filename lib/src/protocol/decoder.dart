import 'dart:typed_data';

class Decoder {
  bool readBool(ByteData buffer, int offset) {
    final value = buffer.getInt8(offset);
    return value > 0 ? true : false;
  }

  ({int value, int bytesRead}) readVarint(List<int> byteArray, int offset,
      {bool signed = true}) {
    int value = 0;
    int shift = 0;
    int pos = offset;
    int bytesRead = 0;

    while (true) {
      if (pos >= byteArray.length) {
        throw Exception("Unexpected end of data while reading VarInt.");
      }

      int byte = byteArray[pos];
      pos++;
      bytesRead++;

      value |= (byte & 0x7F) << shift;
      shift += 7;

      if ((byte & 0x80) == 0) break;
    }

    if (signed) {
      value = (value >>> 1) ^ -(value & 1);
    }

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
}
