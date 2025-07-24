import 'dart:convert';

import 'package:dart_kafka/dart_kafka.dart';

class ListOffsetResponse {
  final int throttleTimeMs;
  final List<Topic> topics;

  ListOffsetResponse({required this.throttleTimeMs, required this.topics});

  @override
  String toString() {
    return "ListOffsetResponse -> throttleTimeMs: $throttleTimeMs, topics: $topics";
  }

  Map<String, dynamic> toMap() {
    return {
      'throttleTimeMs': throttleTimeMs,
      'topics': topics
          .map(
            (e) => e.toMap(),
          )
          .toList()
    };
  }

  factory ListOffsetResponse.fromMap(Map<String, dynamic> map) {
    return ListOffsetResponse(
      throttleTimeMs: map['throttleTimeMs'],
      topics: (map['topics'] as List<dynamic>)
          .map(
            (e) => Topic.fromMap(e),
          )
          .toList(),
    );
  }

  String toJson() => jsonEncode(toMap());

  factory ListOffsetResponse.fromJson(String source) =>
      ListOffsetResponse.fromMap(jsonDecode(source));
}
