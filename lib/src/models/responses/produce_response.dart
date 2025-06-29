// ignore_for_file: public_member_api_docs, sort_constructors_first
import 'dart:convert';

import 'package:dart_kafka/src/models/components/produce_response_component.dart';

class ProduceResponse {
  final List<ProduceResponseComponent> responses;
  final int? throttleTimeMs;

  ProduceResponse({required this.responses, this.throttleTimeMs});

  @override
  String toString() {
    return "ProduceResponse -> responses: $responses, throttleTimeMs: $throttleTimeMs";
  }

  Map<String, dynamic> toMap() {
    return <String, dynamic>{
      'responses': responses.map((x) => x.toMap()).toList(),
      'throttleTimeMs': throttleTimeMs,
    };
  }

  factory ProduceResponse.fromMap(Map<String, dynamic> map) {
    return ProduceResponse(
      responses: List<ProduceResponseComponent>.from(
        (map['responses'] as List<dynamic>).map<ProduceResponseComponent>(
          (x) => ProduceResponseComponent.fromMap(x as Map<String, dynamic>),
        ),
      ),
      throttleTimeMs:
          map['throttleTimeMs'] != null ? map['throttleTimeMs'] as int : null,
    );
  }

  String toJson() => json.encode(toMap());

  factory ProduceResponse.fromJson(String source) =>
      ProduceResponse.fromMap(json.decode(source) as Map<String, dynamic>);
}
