import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/definitions/types.dart';

class Request {
  final int apiKey;
  final int apiVersion;
  final Uint8List message;
  final Deserializer function;
  final String? topicName;
  final int? partition;
  final int correlationId;
  final bool async;
  final Socket? broker;

  final bool autoCommit;
  final String? groupId;
  final String? memberId;
  final String? groupInstanceId;
  final Topic? topic;

  Request({
    required this.correlationId,
    required this.apiKey,
    required this.apiVersion,
    required this.message,
    required this.function,
    required this.async,
    this.topicName,
    this.partition,
    this.broker,
    this.groupId,
    this.groupInstanceId,
    this.memberId,
    this.autoCommit = false,
    this.topic,
  }) {
    if (autoCommit && (memberId == null || groupId == null)) {
      throw Exception(
          "Tryed to create an autoCommit request without memberId or groupId");
    }
  }
}
