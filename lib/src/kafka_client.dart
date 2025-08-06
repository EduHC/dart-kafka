import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import '../dart_kafka.dart';
import 'definitions/types.dart';
import 'kafka_cluster.dart';
import 'models/request.dart';
import 'protocol/endocer.dart';
import 'protocol/traffic_controller.dart';
import 'protocol/utils.dart';

class KafkaClient {
  /// @Parameter brokers
  ///    A list containing only the Host Address and the Port to access N Kafka Brokers
  KafkaClient({
    required List<Broker> brokers,
    required this.rebalanceTimeoutMs,
    required this.sessionTimeoutMs,
    this.clientId,
  }) {
    _cluster.setBrokers(brokers);
    _trafficControler = TrafficControler(
      cluster: _cluster,
      eventController: _eventController,
      kafka: this,
      admin: getAdminClient(),
    );
  }
  late final TrafficControler _trafficControler;

  final StreamController _eventController = StreamController();
  final KafkaCluster _cluster = KafkaCluster();
  final List<Broker> brokers = [];
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final String? clientId;
  late final KafkaAdmin _admin;
  late final KafkaConsumer _consumer;
  late final KafkaProducer _producer;
  final int rebalanceTimeoutMs;
  final int sessionTimeoutMs;

  Stream get eventStream => _eventController.stream.asBroadcastStream();
  bool get hasPendingProcesses => _trafficControler.hasPendingProcesses;
  List<String> get topicsInUse => _cluster.topicsInUse;

  bool _started = false;
  bool _consumerStarted = false;
  bool _producerStarted = false;
  bool _adminStarted = false;

  bool get isKafkaStarted => _started;
  bool get isConsumerStarted => _consumerStarted;
  bool get isProducerStarted => _producerStarted;
  bool get isAdminStarted => _adminStarted;

  Future<void> connect() async {
    if (_started) return;
    await _cluster.connect(responseHandler: _handleResponse);
    _started = true;
  }

  Future<void> close() async {
    while (hasPendingProcesses) {
      await Future.delayed(const Duration(seconds: 1));
      continue;
    }
    _cluster.close();
  }

  // Request/Response controllers
  void _handleResponse(Uint8List response) {
    _trafficControler
      ..enqueueBrokerMessage(response)
      ..drainBrokerMessagesQueue();
  }

  // Broker Related functions
  Socket getBrokerForPartition({
    required String topic,
    required int partition,
  }) =>
      _cluster.getBrokerForPartition(topic: topic, partition: partition);

  Future<void> updateTopicsBroker({required MetadataResponse metadata}) async {
    await _cluster.updateTopicsBroker(metadata: metadata);
  }

  Socket getAnyBroker() => _cluster.getAnyBroker();

  Socket? getBrokerByHost({required String host, required int port}) =>
      _cluster.getBrokerByHost(host: host, port: port);

  Future<dynamic> enqueueRequest({
    required Uint8List message,
    required int correlationId,
    required int apiKey,
    required int apiVersion,
    required Deserializer function,
    String? topicName,
    int? partition,
    bool async = true,
    Socket? broker,
    String? groupId,
    String? memberId,
    String? groupInstanceId,
    Topic? topic,
    bool autoCommit = false,
  }) async {
    final Future<dynamic> res = _trafficControler.enqueuePendindRequest(
      message: message,
      correlationId: correlationId,
      apiKey: apiKey,
      apiVersion: apiVersion,
      function: function,
      topicName: topicName,
      partition: partition,
      async: async,
      broker: broker,
      groupId: groupId,
      memberId: memberId,
      groupInstanceId: groupInstanceId,
      topic: topic,
      autoCommit: autoCommit,
    );

    if (async) return;

    return res;
  }

  KafkaConsumer getConsumerClient() {
    if (isConsumerStarted) return _consumer;

    _consumer = KafkaConsumer(kafka: this);

    _consumerStarted = true;
    return _consumer;
  }

  KafkaProducer getProducerClient() {
    if (isProducerStarted) return _producer;

    _producer = KafkaProducer(kafka: this);

    _producerStarted = true;
    return _producer;
  }

  KafkaAdmin getAdminClient() {
    if (isAdminStarted) return _admin;

    _admin = KafkaAdmin(kafka: this);
    _adminStarted = true;

    return _admin;
  }

  Future<dynamic> commitMessage({required Request req}) async {
    // TODO(Eduardo): Finalize the implementation of Auto Commit messages
    if (req.topic == null) {
      throw Exception(
        'Tryied to commit and request without the Topic information.',
      );
    }
    final List<OffsetCommitPartition> partitions = [];
    for (final Partition part in req.topic!.partitions ?? []) {
      partitions.add(
        OffsetCommitPartition(
          id: part.id,
          commitedOffset: part.fetchOffset!,
          commitedLeaderEpoch: -1,
        ),
      );
    }

    _consumer.sendOffsetCommit(
      groupId: req.groupId!,
      memberId: req.memberId!,
      groupInstanceId: req.groupInstanceId,
      topics: [
        OffsetCommitTopic(
          name: req.topic!.topicName,
          partitions: partitions,
        ),
      ],
    );
  }
}
