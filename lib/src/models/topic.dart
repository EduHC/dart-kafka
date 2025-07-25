// ignore_for_file: public_member_api_docs, sort_constructors_first
import 'dart:convert';

import 'package:dart_kafka/src/models/partition.dart';

class Topic {
  final String topicName;
  final List<Partition>? partitions;

  Topic({required this.topicName, required this.partitions});

  @override
  String toString() {
    return "FetchTopic -> topicName: $topicName, FetchPartitions: $partitions";
  }

  Map<String, dynamic> toMap() {
    return <String, dynamic>{
      'topicName': topicName,
      'partitions': partitions?.map((x) => x.toMap()).toList(),
    };
  }

  factory Topic.fromMap(Map<String, dynamic> map) {
    return Topic(
      topicName: map['topicName'] as String,
      partitions: map['partitions'] != null
          ? List<Partition>.from(
              (map['partitions'] as List<dynamic>).map<Partition?>(
                (x) => Partition.fromMap(x as Map<String, dynamic>),
              ),
            )
          : null,
    );
  }

  String toJson() => json.encode(toMap());

  factory Topic.fromJson(String source) =>
      Topic.fromMap(json.decode(source) as Map<String, dynamic>);
}
