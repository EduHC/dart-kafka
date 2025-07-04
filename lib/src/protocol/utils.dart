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
