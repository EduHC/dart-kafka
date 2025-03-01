class AbortedTransaction {
  final int producerId;
  final int firstOffset;

  AbortedTransaction({required this.producerId, required this.firstOffset});

  @override
  String toString() {
    return "AbortedTransaction -> producerId: $producerId, firstOffset: $firstOffset";
  }
}
