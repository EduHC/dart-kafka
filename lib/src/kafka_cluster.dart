import 'dart:async';
import 'dart:io';

import 'package:dart_kafka/dart_kafka.dart';

class KafkaCluster {
  final Set<Broker> _brokers = {};
  // {'testeomnilightvitaverse.location': '192.168.200.31:29092'}
  final Map<String, Map<int, String>> _topicsBrokers = {};
  // {'192.168.200.31:29092': Socket::class}
  final Map<String, Socket> _sockets = {};
  final Set<StreamSubscription> _subscriptions = {};

  List<String> get topicsInUse => _topicsBrokers.keys.toList();

  Future<void> connect({required Function responseHandler}) async {
    for (Broker broker in _brokers) {
      try {
        String key = "${broker.host}:${broker.port}";

        if (_sockets.containsKey(key)) continue;
        Socket sock = await Socket.connect(broker.host, broker.port);
        _sockets.addAll({key: sock});
      } catch (e) {
        throw Exception("Error trying to connect to informed host: $e");
      }
    }

    for (Socket sock in _sockets.values) {
      StreamSubscription subscription = sock.listen(
        (event) => responseHandler(event),
      );
      _subscriptions.add(subscription);
    }
  }

  void close() {
    _sockets.forEach(
      (key, value) => value.close,
    );
    _subscriptions.map(
      (element) => element.cancel,
    );
    _sockets.clear();
    _topicsBrokers.clear();
    _brokers.clear();
    _subscriptions.clear();
  }

  void addBroker(Broker broker) {
    _brokers.add(broker);
  }

  void setBrokers(List<Broker> brokers) {
    _brokers.clear();
    _brokers.addAll(brokers);
  }

  Future<void> closeBroker({required brokerId}) async {
    if (!_sockets.containsKey(brokerId)) {
      throw Exception("Requested Broker not found!");
    }

    await _sockets[brokerId]!.close();
    _sockets.remove(brokerId);
  }

  Socket getBrokerForPartition({
    required String topic,
    required int partition,
  }) {
    if (!_topicsBrokers.containsKey(topic)) {
      throw Exception("Topic not found in the Cluster! $topic");
    }

    String? brokerRoute = (_topicsBrokers[topic] as Map)[partition];

    if (brokerRoute == null || brokerRoute.isEmpty) {
      throw Exception(
          "Not found Broker Host and Port for topic $topic and partition $partition");
    }

    if (!_sockets.containsKey(brokerRoute)) {
      throw Exception(
          "Socket ${_topicsBrokers[topic]} not found for the topic $topic");
    }

    return _sockets[brokerRoute]!;
  }

  Socket getAnyBroker() {
    if (_sockets.isEmpty) {
      throw Exception("No Brokers available!");
    }
    return _sockets.values.first;
  }

  Socket? getBrokerByHost({required String host, required int port}) {
    return _sockets['$host:$port'];
  }

  Future<void> updateTopicsBroker({required MetadataResponse metadata}) async {
    print("[DART-KAFKA] Entrou para atualziar topicos no Broker!");
    print('[DART-KAFKA] Topics Antes de alterar: $_topicsBrokers');
    // print('[DART-KAFKA] Recebido: $metadata');
    _topicsBrokers.clear();
    Map? brokers = {
      for (var b in metadata.brokers) b.nodeId: "${b.host}:${b.port}"
    };

    for (KafkaTopicMetadata topic in metadata.topics) {
      for (KafkaPartitionMetadata partition in topic.partitions) {
        _topicsBrokers.addAll({
          topic.topicName: {
            partition.partitionId: "${brokers[partition.leaderId]}"
          }
        });
      }
    }

    print('[DART-KAFKA] Topics Depois de alterar: $_topicsBrokers');
    brokers.clear();
    brokers = null;
  }
}
