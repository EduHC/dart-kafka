import 'dart:math';

class UUID {
  final int msb;
  final int lsb;

  UUID(this.msb, this.lsb);

  /// Generates a Type 4 (random) UUID.
  static UUID v4({bool zero = false}) {
    if (zero) {
      return UUID(0, 0);
    }

    final Random random = Random.secure();

    // Generate 64 random bits
    int msb = (random.nextInt(1 << 32) << 32) | random.nextInt(1 << 32);
    int lsb = (random.nextInt(1 << 32) << 32) | random.nextInt(1 << 32);

    // Set version to 4 (UUID version field)
    msb = (msb & 0xFFFFFFFFFFFF0FFF) | (0x4 << 12);

    // Set variant bits to `10xx` (UUID variant field)
    lsb = (lsb & 0x3FFFFFFFFFFFFFFF) | (0x8000000000000000);

    return UUID(msb, lsb);
  }

  @override
  String toString() {
    return '${_toHex(msb >> 32, 8)}-${_toHex(msb >> 16, 4)}-${_toHex(msb, 4)}-'
        '${_toHex(lsb >> 48, 4)}-${_toHex(lsb, 12)}';
  }

  String _toHex(int value, int width) =>
      value.toRadixString(16).padLeft(width, '0');
}
