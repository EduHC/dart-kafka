import 'package:dart_kafka/dart_kafka.dart';

class ProduceResponseComponent {
  final String topicName;
  final List<Partition> partitions;

  ProduceResponseComponent({required this.topicName, required this.partitions});

  @override
  String toString() {
    return "ResponseComponent -> topicName: $topicName, partitions: $partitions";
  }
}
