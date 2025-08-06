// ignore_for_file: public_member_api_docs, sort_constructors_first
import 'dart:convert';

import '../../../dart_kafka.dart';

class ProduceResponseComponent {
  final String topicName;
  final List<Partition> partitions;

  ProduceResponseComponent({required this.topicName, required this.partitions});

  @override
  String toString() =>
      'ResponseComponent -> topicName: $topicName, partitions: $partitions';

  Map<String, dynamic> toMap() => <String, dynamic>{
        'topicName': topicName,
        'partitions': partitions.map((x) => x.toMap()).toList(),
      };

  factory ProduceResponseComponent.fromMap(Map<String, dynamic> map) =>
      ProduceResponseComponent(
        topicName: map['topicName'] as String,
        partitions: List<Partition>.from(
          (map['partitions'] as List<dynamic>).map<Partition>(
            (x) => Partition.fromMap(x as Map<String, dynamic>),
          ),
        ),
      );

  String toJson() => json.encode(toMap());

  factory ProduceResponseComponent.fromJson(String source) =>
      ProduceResponseComponent.fromMap(
        json.decode(source) as Map<String, dynamic>,
      );
}
