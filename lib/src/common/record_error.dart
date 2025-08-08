import 'dart:convert';

class RecordError {
  RecordError({required this.batchIndex, this.errorMessage});

  factory RecordError.fromMap(Map<String, dynamic> map) => RecordError(
        batchIndex: map['batchIndex'] as int,
        errorMessage:
            map['errorMessage'] != null ? map['errorMessage'] as String : null,
      );

  factory RecordError.fromJson(String source) =>
      RecordError.fromMap(json.decode(source) as Map<String, dynamic>);
  final int batchIndex;
  final String? errorMessage;

  @override
  String toString() =>
      'RecordError -> batchIndex: $batchIndex, errorMessage: $errorMessage';

  Map<String, dynamic> toMap() => <String, dynamic>{
        'batchIndex': batchIndex,
        'errorMessage': errorMessage,
      };

  String toJson() => json.encode(toMap());
}
