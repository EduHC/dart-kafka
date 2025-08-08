import 'dart:convert';

import 'partition.dart';

class ProduceResponseComponent {
  ProduceResponseComponent({required this.topicName, required this.partitions});

  factory ProduceResponseComponent.fromMap(Map<String, dynamic> map) =>
      ProduceResponseComponent(
        topicName: map['topicName'] as String,
        partitions: List<Partition>.from(
          (map['partitions'] as List<dynamic>).map<Partition>(
            (x) => Partition.fromMap(x as Map<String, dynamic>),
          ),
        ),
      );

  factory ProduceResponseComponent.fromJson(String source) =>
      ProduceResponseComponent.fromMap(
        json.decode(source) as Map<String, dynamic>,
      );
  final String topicName;
  final List<Partition> partitions;

  @override
  String toString() =>
      'ResponseComponent -> topicName: $topicName, partitions: $partitions';

  Map<String, dynamic> toMap() => <String, dynamic>{
        'topicName': topicName,
        'partitions': partitions.map((x) => x.toMap()).toList(),
      };

  String toJson() => json.encode(toMap());
}
