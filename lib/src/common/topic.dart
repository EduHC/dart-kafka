import 'dart:convert';

import 'partition.dart';

class Topic {
  Topic({required this.topicName, required this.partitions});

  factory Topic.fromMap(Map<String, dynamic> map) => Topic(
        topicName: map['topicName'] as String,
        partitions: map['partitions'] != null
            ? List<Partition>.from(
                (map['partitions'] as List<dynamic>).map<Partition?>(
                  (x) => Partition.fromMap(x as Map<String, dynamic>),
                ),
              )
            : null,
      );

  factory Topic.fromJson(String source) =>
      Topic.fromMap(json.decode(source) as Map<String, dynamic>);
  final String topicName;
  final List<Partition>? partitions;

  @override
  String toString() =>
      'FetchTopic -> topicName: $topicName, FetchPartitions: $partitions';

  Map<String, dynamic> toMap() => <String, dynamic>{
        'topicName': topicName,
        'partitions': partitions?.map((x) => x.toMap()).toList(),
      };

  String toJson() => json.encode(toMap());
}
