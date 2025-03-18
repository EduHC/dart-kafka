import 'package:dart_kafka/src/models/components/produce_response_component.dart';

class ProduceResponse {
  final List<ProduceResponseComponent> responses;
  final int? throttleTimeMs;

  ProduceResponse({required this.responses, this.throttleTimeMs});

  @override
  String toString() {
    return "ProduceResponse -> responses: $responses, throttleTimeMs: $throttleTimeMs";
  }
}

