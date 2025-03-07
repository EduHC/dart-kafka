import 'dart:typed_data';

class CRC32C {
  static final List<int> _table = _generateTable();

  static int calculate(Uint8List data, [int crc = 0]) {
    crc ^= 0xFFFFFFFF;
    for (var byte in data) {
      crc = _table[(crc ^ byte) & 0xFF] ^ (crc >>> 8);
    }
    return crc ^ 0xFFFFFFFF;
  }

  static List<int> _generateTable() {
    const int polynomial = 0x82F63B78;
    List<int> table = List.filled(256, 0);

    for (int i = 0; i < 256; i++) {
      int crc = i;
      for (int j = 0; j < 8; j++) {
        if ((crc & 1) != 0) {
          crc = (crc >>> 1) ^ polynomial;
        } else {
          crc >>>= 1;
        }
      }
      table[i] = crc;
    }

    return table;
  }
}
