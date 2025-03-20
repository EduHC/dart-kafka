import 'dart:async';
import 'dart:collection';
import 'dart:io';

import 'package:dart_kafka/dart_kafka.dart';

class KafkaCluster {
  final Set<Broker> _brokers = {};
  final Set<KafkaTopicMetadata> _topics = {};
  final HashMap<String, Socket> _sockets = HashMap();
  final Set<StreamSubscription> _subscriptions = {};

  void addBroker(Broker broker) {
    _brokers.add(broker);
  }

  void setBrokers(List<Broker> brokers) {
    _brokers.clear();
    _brokers.addAll(brokers);
  }

  void addTopic(KafkaTopicMetadata topic) {
    _topics.add(topic);
  }

  void setTopics(List<KafkaTopicMetadata> topics) {
    _topics.clear();
    _topics.addAll(topics);
  }

  Socket getBrokerById(int id) {
    if (!_sockets.containsKey(id.toString())) {
      throw Exception("Requested Broker not found!");
    }
    return _sockets[id.toString()]!;
  }

  Future<void> closeBroker({required brokerId}) async {
    if (!_sockets.containsKey(brokerId)) {
      throw Exception("Requested Broker not found!");
    }

    await _sockets[brokerId]!.close();
    _sockets.remove(brokerId);
  }

  Future<void> connect({required Function responseHanddler}) async {
    _brokers.map(
      (Broker broker) async {
        try {
          String key = "${broker.host}:${broker.port}";
          if (!_sockets.containsKey(key)) {
            Socket sock = await Socket.connect(broker.host, broker.port);
            _sockets.addAll({key: sock});

            Future.microtask(() {
              StreamSubscription subscription = sock.listen(
                (event) => responseHanddler(event),
              );
              _subscriptions.add(subscription);
            });
          }
        } catch (e) {
          throw Exception("Error trying to connect to informed host: $e");
        }
      },
    );
  }

  void close() {
    _sockets.forEach(
      (key, value) => value.close,
    );
    _subscriptions.map(
      (element) => element.cancel,
    );
    _sockets.clear();
    _topics.clear();
    _brokers.clear();
    _subscriptions.clear();
  }

  Socket? getLeaderBroker(String topic) {
    final KafkaTopicMetadata metadata = _topics.firstWhere(
      (element) => element.topicName == topic,
    );

    for (KafkaPartitionMetadata pMetadata in metadata.partitions) {}

    return null;
  }
}
