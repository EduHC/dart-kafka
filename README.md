# Kafka Dart
***[STATUS] Work in progress!***

**License:** MIT

A pure Dart implementation of the Apache Kafka Protocol from scratch. This project provides a lightweight, efficient, and easy-to-use Kafka client for Dart applications. It includes support for producing, consuming, and administering Kafka topics, all implemented natively in Dart.

## Features

- **KafkaProducer:** Produce messages to Kafka topics.
- **KafkaConsumer:** Consume messages from Kafka topics.
- **KafkaAdmin:** Administer Kafka topics, partitions, and configurations.
- **KafkaClient:** Core client for interacting with Kafka brokers.
- **MIT Licensed:** Free to use, modify, and distribute.

## Installation

Add the package to your `pubspec.yaml`:

```yaml
dependencies:
  kafka_dart:
    git: https://github.com/EduHC/dart-kafka.git
```

Then, run `dart pub get` to install the package.

## Usage

### 1. KafkaClient
The core TPC connection and response handler:

```dart
import 'package:dart_kafka/dart_kafka.dart';

void main() async {
  KafkaClient kafka = KafkaClient(host: '192.168.3.55', port: 29092);
  await kafka.connect();

  kafka.addPendingRequest(
        correlationId: correlationId,
        deserializer: metadataApi.deserialize);

  kafka.completeRequest(correlationId);
  await client.close();
}
```

### 2. KafkaProducer
Produce messages to a Kafka topic:

```dart
import 'package:dart_kafka/dart_kafka.dart';

void main() async {
    final KafkaClient kafka = KafkaClient(host: '192.168.3.55', port: 29092);
    await kafka.connect();
    
    final producer = KafkaProducer(kafka: kafka);

    await producer.send(
        topic: 'test-topic',
        key: 'message-key',
        value: 'Hello, Kafka!',
    );

    await kafka.close();
}
```

### 3. KafkaConsumer
Consume messages from a Kafka topic:

```dart
import 'package:dart_kafka/dart_kafka.dart';

void main() async {
    final KafkaClient kafka = KafkaClient(host: '192.168.3.55', port: 29092);
    await kafka.connect();
    
    final consumer = KafkaConsumer(kafka: kafka);

    List<Topic> topics = [
    Topic(
        topicName: 'test-topic',
        partitions: [Partition(partitionId: 0, logStartOffset: 0)])
    ];
    await consumer.sendFetchRequest(
        clientId: 'consumer',
        topics: topics, 
        isolationLevel: 0, 
        apiVersion: 8
    );

    await kafka.close();
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