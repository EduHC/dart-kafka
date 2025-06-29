// ignore_for_file: public_member_api_docs, sort_constructors_first
import 'dart:convert';

class AbortedTransaction {
  final int producerId;
  final int firstOffset;

  AbortedTransaction({required this.producerId, required this.firstOffset});

  @override
  String toString() {
    return "AbortedTransaction -> producerId: $producerId, firstOffset: $firstOffset";
  }

  Map<String, dynamic> toMap() {
    return <String, dynamic>{
      'producerId': producerId,
      'firstOffset': firstOffset,
    };
  }

  factory AbortedTransaction.fromMap(Map<String, dynamic> map) {
    return AbortedTransaction(
      producerId: map['producerId'] as int,
      firstOffset: map['firstOffset'] as int,
    );
  }

  String toJson() => json.encode(toMap());

  factory AbortedTransaction.fromJson(String source) =>
      AbortedTransaction.fromMap(json.decode(source) as Map<String, dynamic>);
}
