// ignore_for_file: public_member_api_docs, sort_constructors_first
import 'dart:convert';

class RecordError {
  final int batchIndex;
  final String? errorMessage;

  RecordError({required this.batchIndex, this.errorMessage});

  @override
  String toString() {
    return "RecordError -> batchIndex: $batchIndex, errorMessage: $errorMessage";
  }

  Map<String, dynamic> toMap() {
    return <String, dynamic>{
      'batchIndex': batchIndex,
      'errorMessage': errorMessage,
    };
  }

  factory RecordError.fromMap(Map<String, dynamic> map) {
    return RecordError(
      batchIndex: map['batchIndex'] as int,
      errorMessage:
          map['errorMessage'] != null ? map['errorMessage'] as String : null,
    );
  }

  String toJson() => json.encode(toMap());

  factory RecordError.fromJson(String source) =>
      RecordError.fromMap(json.decode(source) as Map<String, dynamic>);
}
