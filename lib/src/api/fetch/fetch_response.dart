import 'dart:convert';

import '../../common/topic.dart';

class FetchResponse {
  FetchResponse({
    required this.throttleTimeMs,
    required this.errorCode,
    required this.sessionId,
    required this.topics,
  });

  factory FetchResponse.fromMap(Map<String, dynamic> map) => FetchResponse(
        throttleTimeMs: map['throttleTimeMs'] as int,
        errorCode: map['errorCode'] as int,
        sessionId: map['sessionId'] as int,
        topics: List<Topic>.from(
          (map['topics'] as List<dynamic>).map<Topic>(
            (x) => Topic.fromMap(x as Map<String, dynamic>),
          ),
        ),
      );

  factory FetchResponse.fromJson(String source) =>
      FetchResponse.fromMap(json.decode(source) as Map<String, dynamic>);
  final int throttleTimeMs;
  final int errorCode;
  final int sessionId;
  final List<Topic> topics;

  @override
  String toString() =>
      'FetchResponse -> sessionId: $sessionId, throttleTimeMs: $throttleTimeMs, errorCode: $errorCode, topics: $topics';

  Map<String, dynamic> toMap() => <String, dynamic>{
        'throttleTimeMs': throttleTimeMs,
        'errorCode': errorCode,
        'sessionId': sessionId,
        'topics': topics.map((x) => x.toMap()).toList(),
      };

  String toJson() => json.encode(toMap());
}
