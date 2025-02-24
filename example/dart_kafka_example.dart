import 'package:dart_kafka/dart_kafka.dart';

void main() async {
  final kafkaClient = KafkaClient('localhost', 9092);

  try {
    // Step 1: Connect to the Kafka broker
    await kafkaClient.connect();
    print('Connected to Kafka broker');

    // Step 2: Produce a message to a topic
    await kafkaClient.produce('test-topic', 'Hello, Kafka!');
    print('Produced message to test-topic');

    // Step 3: Enable static membership (optional)
    kafkaClient.enableStaticMembership('static-member-1');
    print('Enabled static membership with ID: static-member-1');

    // Step 4: Subscribe to a topic as part of a consumer group
    await kafkaClient.subscribe('test-group', ['test-topic']);
    print('Subscribed to test-topic as part of test-group');

    // Step 5: Consume messages from the topic
    await kafkaClient.consume('test-topic');
    print('Consumed messages from test-topic');

    // Step 6: Keep the application running to allow heartbeats and message consumption
    print('Waiting for messages and heartbeats...');
    await Future.delayed(
        Duration(seconds: 30)); // Simulate a long-running consumer

    // Step 7: Gracefully disconnect
    await kafkaClient.disconnect();
    print('Disconnected from Kafka broker');
  } on KafkaException catch (e) {
    print('Kafka error: $e');
  } catch (e) {
    print('Unexpected error: $e');
  } finally {
    // Ensure the client is disconnected
    await kafkaClient.disconnect();
  }
}
