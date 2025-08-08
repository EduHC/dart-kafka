import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/common/request.dart';
import 'package:dart_kafka/src/protocol/definition/type.dart';
import 'package:dart_kafka/src/protocol/encoder.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

import 'mock_kafka_admin.dart';

class MockKafkaClient implements KafkaClient {
  final Map<int, Function> _apiResponders = {};
  final List<Map<String, dynamic>> _enqueuedRequests = [];

  void setResponder(int apiKey, Function responder) {
    _apiResponders[apiKey] = responder;
  }

  List<Map<String, dynamic>> get enqueuedRequests => _enqueuedRequests;

  @override
  String clientId = 'mock_client';

  @override
  int rebalanceTimeoutMs = 30000;

  @override
  int sessionTimeoutMs = 60000;

  @override
  Future<dynamic> enqueueRequest({
    required int apiKey,
    required int apiVersion,
    required int correlationId,
    required Deserializer function,
    required Uint8List message,
    bool async = true,
    Socket? broker,
    String? topicName,
    int? partition,
    bool? autoCommit,
    String? groupId,
    String? memberId,
    String? groupInstanceId,
    Topic? topic,
    int? generationId,
  }) {
    final requestData = {
      'apiKey': apiKey,
      'apiVersion': apiVersion,
      'correlationId': correlationId,
      'async': async,
      'topicName': topicName,
      'partition': partition,
    };
    _enqueuedRequests.add(requestData);

    if (async) {
      return Future.value();
    }

    if (_apiResponders.containsKey(apiKey)) {
      final response = _apiResponders[apiKey]!();
      return Future.value(response);
    }

    return Future.error('No mock responder for apiKey $apiKey');
  }

  @override
  KafkaAdmin getAdminClient() => MockKafkaAdmin(this);

  @override
  Socket? getBrokerByHost({required String host, required int port}) => null;

  @override
  Future<void> connect() => throw UnimplementedError();

  Stream<RecordBatch> consume({
    required String topicName,
    required int partition,
  }) =>
      throw UnimplementedError();

  Future<ProduceResponse> produce({
    required String topicName,
    required int partition,
    required List<Record> records,
  }) =>
      throw UnimplementedError();

  Stream<RecordBatch> stream() {
    throw UnimplementedError();
  }

  @override
  Future<void> close() => throw UnimplementedError();

  @override
  Future<dynamic> commitMessage({required Request req}) =>
      throw UnimplementedError();

  @override
  Socket getAnyBroker() => throw UnimplementedError();

  @override
  Socket getBrokerForPartition({
    required int partition,
    required String topic,
  }) =>
      throw UnimplementedError();

  Future<List<Partition>> getPartitionsForTopic(String topicName) =>
      throw UnimplementedError();

  Future<void> initialize() => throw UnimplementedError();

  bool isConnected(Socket? s) => throw UnimplementedError();

  void processMessage({
    required Uint8List message,
    required int correlationId,
    required String host,
    required int port,
  }) {}

  void setBroker({
    required String host,
    required int port,
    required Socket socket,
  }) {}

  List<String> get bootstrapServers => throw UnimplementedError();

  @override
  List<Broker> get brokers => throw UnimplementedError();

  @override
  KafkaConsumer getConsumerClient() => throw UnimplementedError();

  @override
  KafkaProducer getProducerClient({
    int? maxRetries,
    int? requestTimeout,
    int? acks,
    int? maxBlockMs,
    int? maxInFlightRequestsPerConnection,
    int? retryBackoffMs,
  }) =>
      throw UnimplementedError();

  Map<String, Topic> get topicMetadata => throw UnimplementedError();

  @override
  Encoder get encoder => throw UnimplementedError();

  @override
  Future<void> updateTopicsBroker({required MetadataResponse metadata}) {
    throw UnimplementedError();
  }

  @override
  Stream get eventStream => const Stream.empty();

  @override
  bool get hasPendingProcesses => false;

  @override
  bool get isAdminStarted => false;

  @override
  bool get isConsumerStarted => false;

  @override
  bool get isProducerStarted => false;

  @override
  bool get isKafkaStarted => false;

  @override
  List<String> get topicsInUse => [];

  @override
  Utils get utils => Utils();

  @override
  bool get autoCommit => true;
}
