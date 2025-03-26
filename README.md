# Kafka Dart
***[STATUS] Work in progress!***

**License:** MIT

A pure Dart implementation of the Apache Kafka Protocol from scratch. This project provides a lightweight, efficient, and easy-to-use Kafka client for Dart applications. It includes support for producing, consuming, and administering Kafka topics, all implemented natively in Dart.

## Main Classes

- **KafkaProducer:** Produce messages to Kafka topics.
- **KafkaConsumer:** Consume messages from Kafka topics.
- **KafkaAdmin:** Administer Kafka topics, partitions, and configurations.
- **KafkaClient:** Core client for handling generic controls such as queues, conncect and close.
- **KafkaCluster:** Responsible for handling interaction with Kafka Brokers.

## Installation

Add the package to your `pubspec.yaml`:

```yaml
dependencies:
  kafka_dart:
    git: https://github.com/EduHC/dart-kafka.git
```

Then, run `dart pub get` to install the package.

## Usage

### 2. KafkaProducer
Produce messages to a Kafka topic:

```dart
import 'package:dart_kafka/dart_kafka.dart';

void main() async {
  List<Broker> brokers = [
    Broker(host: '192.168.200.131', port: 29092),
    Broker(host: '192.168.200.131', port: 29093),
    Broker(host: '192.168.200.131', port: 29094),
  ];
  final KafkaClient kafka = KafkaClient(brokers: brokers);
  await kafka.connect();

  Future.microtask(
    () => kafka.eventStream.listen(
      (event) => print("Received from Stream: $event"),
    ),
  );

  KafkaAdmin admin = KafkaAdmin(kafka: kafka);
  await admin.updateTopicsMetadata(
      topics: [
        'test-topic',
        'notifications',
      ],
      async: false,
      apiVersion: 9,
      clientId: 'dart-kafka',
      allowAutoTopicCreation: false,
      includeClusterAuthorizedOperations: false,
      includeTopicAuthorizedOperations: false,
      correlationId: null);

  KafkaProducer producer = KafkaProducer(kafka: kafka);

  producer.produce(
      acks: -1,
      timeoutMs: 1500,
      topics: [
        Topic(topicName: 'notifications', partitions: [
          Partition(
              id: 0,
              batch: RecordBatch(records: [
                Record(
                    attributes: 0,
                    timestampDelta: 0,
                    offsetDelta: 0,
                    timestamp: DateTime.now().millisecondsSinceEpoch,
                    value: '{"id": 1}')
              ]))
        ]),
      ],
      async: true,
      apiVersion: 11,
      clientId: 'dart-kafka',
      producerId: -1,
      attributes: 0);

  dynamic res = await producer.produce(
      acks: -1,
      timeoutMs: 1500,
      topics: [
        Topic(topicName: 'test-topic', partitions: [
          Partition(
              id: 0,
              batch: RecordBatch(records: [
                Record(
                    attributes: 0,
                    timestampDelta: 0,
                    offsetDelta: 0,
                    timestamp: DateTime.now().millisecondsSinceEpoch,
                    value: '{"test": "This is a test!"}')
              ]))
        ]),
      ],
      async: false,
      apiVersion: 11,
      clientId: 'dart-kafka',
      producerId: -1,
      attributes: 0);

  print("Sync deu: $res");

  kafka.close();
  return;
}

```

### 2. KafkaConsumer
Consume messages from a Kafka topic:

```dart
import 'package:dart_kafka/dart_kafka.dart';

void main() async {
  List<Broker> brokers = [
    Broker(host: '192.168.200.131', port: 29092),
    Broker(host: '192.168.200.131', port: 29093),
    Broker(host: '192.168.200.131', port: 29094),
  ];
  final KafkaClient kafka = KafkaClient(brokers: brokers);
  await kafka.connect();

  // Listen to the Streaming events received from Async requests
  Future.microtask(
    () => kafka.eventStream.listen(
      (event) => print("[ASYNC request]: $event"),
    ),
  );

  KafkaConsumer consumer = KafkaConsumer(kafka: kafka);

  consumer.sendFetchRequest(
      clientId: 'dart-kafka',
      apiVersion: 8,
      async: true,
      topics: [
        Topic(topicName: 'test-topic', partitions: [
          Partition(id: 0, fetchOffset: 0),
        ])
      ]);

  KafkaAdmin admin = kafka.admin;
  await admin.updateTopicsMetadata(
      topics: [
        'test-topic',
        'notifications',
      ],
      async: false,
      apiVersion: 9,
      clientId: 'dart-kafka',
      allowAutoTopicCreation: false,
      includeClusterAuthorizedOperations: false,
      includeTopicAuthorizedOperations: false,
      correlationId: null);

  dynamic res = await consumer.sendFetchRequest(
      clientId: 'dart-kafka',
      apiVersion: 8,
      async: false,
      topics: [
        Topic(topicName: 'notifications', partitions: [
          Partition(id: 0, fetchOffset: 0),
        ])
      ]);

  print("*********************************************");
  print("[SYNC Request]: $res");

  kafka.close();
  return;
}


```

### 4. KafkaAdmin
Administer Kafka topics:

```dart
import 'package:dart_kafka/dart_kafka.dart';

void main() async {
    final KafkaClient kafka = KafkaClient(host: '192.168.3.55', port: 29092);
    await kafka.connect();
    
    final admin = KafkaAdmin(kafka: kafka);

    await admin.sendApiVersionRequest(apiVersion: 0, clientId: 'test');
    
    await admin.sendMetadataRequest(
        topics: ['test-topic'],
        allowAutoTopicCreation: true,
        includeClusterAuthorizedOperations: true,
        includeTopicAuthorizedOperations: true,
        clientId: 'test',
        apiVersion: 12
    );

    await kafka.close();
}
```

## Documentation
For mor detailed information about the Apache Kafka's Protocol, please see this link: https://kafka.apache.org/protocol

## Contributing

Contributions are welcome! If you'd like to contribute, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Commit your changes and push to your fork.
4. Submit a pull request with a detailed description of your changes.

## Development Setup

Clone the repository:

```bash
git clone https://github.com/EduHC/dart-kafka
cd dart-kafka
```

Install dependencies:

```bash
dart pub get
```

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Acknowledgments

Inspired by the Apache Kafka Protocol.

Built with ❤️ and Dart.

## Support

If you encounter any issues or have questions, please open an issue.
'