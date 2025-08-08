import 'dart:convert';

class AbortedTransaction {
  AbortedTransaction({required this.producerId, required this.firstOffset});

  factory AbortedTransaction.fromMap(Map<String, dynamic> map) =>
      AbortedTransaction(
        producerId: map['producerId'] as int,
        firstOffset: map['firstOffset'] as int,
      );

  factory AbortedTransaction.fromJson(String source) =>
      AbortedTransaction.fromMap(json.decode(source) as Map<String, dynamic>);
  final int producerId;
  final int firstOffset;

  @override
  String toString() =>
      'AbortedTransaction -> producerId: $producerId, firstOffset: $firstOffset';

  Map<String, dynamic> toMap() => <String, dynamic>{
        'producerId': producerId,
        'firstOffset': firstOffset,
      };

  String toJson() => json.encode(toMap());
}
