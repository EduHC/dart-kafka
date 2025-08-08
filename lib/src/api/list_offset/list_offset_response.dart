import 'dart:convert';

import '../../common/topic.dart';

class ListOffsetResponse {
  ListOffsetResponse({required this.throttleTimeMs, required this.topics});

  factory ListOffsetResponse.fromMap(Map<String, dynamic> map) =>
      ListOffsetResponse(
        throttleTimeMs: map['throttleTimeMs'],
        topics: (map['topics'] as List<dynamic>)
            .map(
              (e) => Topic.fromMap(e),
            )
            .toList(),
      );

  factory ListOffsetResponse.fromJson(String source) =>
      ListOffsetResponse.fromMap(jsonDecode(source));
  final int throttleTimeMs;
  final List<Topic> topics;

  @override
  String toString() =>
      'ListOffsetResponse -> throttleTimeMs: $throttleTimeMs, topics: $topics';

  Map<String, dynamic> toMap() => {
        'throttleTimeMs': throttleTimeMs,
        'topics': topics
            .map(
              (e) => e.toMap(),
            )
            .toList(),
      };

  String toJson() => jsonEncode(toMap());
}
