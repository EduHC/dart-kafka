import 'dart:convert';

import '../../common/produce_response_component.dart';

class ProduceResponse {
  ProduceResponse({required this.responses, this.throttleTimeMs});

  factory ProduceResponse.fromMap(Map<String, dynamic> map) => ProduceResponse(
        responses: List<ProduceResponseComponent>.from(
          (map['responses'] as List<dynamic>).map<ProduceResponseComponent>(
            (x) => ProduceResponseComponent.fromMap(x as Map<String, dynamic>),
          ),
        ),
        throttleTimeMs:
            map['throttleTimeMs'] != null ? map['throttleTimeMs'] as int : null,
      );

  factory ProduceResponse.fromJson(String source) =>
      ProduceResponse.fromMap(json.decode(source) as Map<String, dynamic>);
  final List<ProduceResponseComponent> responses;
  final int? throttleTimeMs;

  @override
  String toString() =>
      'ProduceResponse -> responses: $responses, throttleTimeMs: $throttleTimeMs';

  Map<String, dynamic> toMap() => <String, dynamic>{
        'responses': responses.map((x) => x.toMap()).toList(),
        'throttleTimeMs': throttleTimeMs,
      };

  String toJson() => json.encode(toMap());
}
