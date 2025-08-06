// ignore_for_file: prefer_constructors_over_static_methods

import 'dart:math';

class UUID {
  UUID(this.msb, this.lsb);

  final int msb;
  final int lsb;

  /// Generates a Type 4 (random) UUID.
  static UUID v4({bool zero = false}) {
    if (zero) return UUID(0, 0);

    final Random random = Random.secure();

    int msb = (random.nextInt(1 << 32) << 32) | random.nextInt(1 << 32);
    int lsb = (random.nextInt(1 << 32) << 32) | random.nextInt(1 << 32);
    msb = (msb & 0xFFFFFFFFFFFF0FFF) | (0x4 << 12);
    lsb = (lsb & 0x3FFFFFFFFFFFFFFF) | 0x8000000000000000;

    return UUID(msb, lsb);
  }

  @override
  String toString() =>
      '${_toHex(msb >> 32, 8)}-${_toHex(msb >> 16, 4)}-${_toHex(msb, 4)}-'
      '${_toHex(lsb >> 48, 4)}-${_toHex(lsb, 12)}';

  String _toHex(int value, int width) =>
      value.toRadixString(16).padLeft(width, '0');
}
