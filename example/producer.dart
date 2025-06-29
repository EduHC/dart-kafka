import 'package:dart_kafka/dart_kafka.dart';
import 'dart:async';

void main() async {
  List<Broker> brokers = [
    Broker(host: '192.168.200.131', port: 29092),
    Broker(host: '192.168.200.131', port: 29093),
    Broker(host: '192.168.200.131', port: 29094),
  ];
  final KafkaClient kafka = KafkaClient(
    brokers: brokers,
    sessionTimeoutMs: 1800000,
    rebalanceTimeoutMs: 1500,
  );

  await kafka.connect();
  KafkaAdmin admin = KafkaAdmin(kafka: kafka);
  KafkaProducer producer = KafkaProducer(kafka: kafka);

  // Listen to Kafka events
  // final subscription = kafka.eventStream.listen((event) {
  //   print("Received from Stream: $event");
  //   _doProduce(producer);
  // });

  await admin.updateTopicsMetadata(
    topics: ['TESTE_EDUARDO'],
    apiVersion: 9,
    allowAutoTopicCreation: false,
    includeClusterAuthorizedOperations: false,
    includeTopicAuthorizedOperations: false,
    correlationId: null,
  );

  // Produce the first message

  _doProduce(producer);

  // Run for 1 minute
  await Future.delayed(Duration(minutes: 1));

  // Cleanup after 1 minute
  // await subscription.cancel();
  await kafka.close();
  print("Kafka closed after 1 minute.");
}

_doProduce(KafkaProducer producer) {
  producer.produce(
    acks: -1,
    timeoutMs: 1500,
    topics: [
      Topic(
        topicName: 'TESTE_EDUARDO',
        partitions: [
          Partition(
            id: 0,
            batch: RecordBatch(
              producerId: null,
              records: [
                Record(
                  attributes: 0,
                  timestampDelta: 0,
                  offsetDelta: 0,
                  timestamp: DateTime.now().millisecondsSinceEpoch,
                  value:
                      "{\"isActive\":1,\"isVitaverse\":1,\"applyerTemperatureOffset\":0,\"naturalTemperatureOffset\":0,\"waterTemperatureOffset\":0,\"waterFlowOffset\":500,\"electricCurrentOffset\":0,\"electricTensionOffset\":0,\"applyerTemperatureMaxOffsetConfigurable\":10,\"electricCurrentMaxOffsetConfigurable\":10,\"naturalTemperatureMaxOffsetConfigurable\":10,\"electricTensionMaxOffsetConfigurable\":20,\"waterFlowMaxOffsetConfigurable\":500,\"waterTemperatureMaxOffsetConfigurable\":10,\"applyerTemperatureStopMin\":-20,\"applyerTemperatureStopMax\":38,\"applyerTemperatureNotifyMin\":-15,\"applyerTemperatureNotifyMax\":35,\"naturalTemperatureStopMin\":0,\"naturalTemperatureStopMax\":45,\"naturalTemperatureNotifyMin\":5,\"naturalTemperatureNotifyMax\":40,\"waterTemperatureStopMin\":10,\"waterTemperatureStopMax\":40,\"waterTemperatureNotifyMin\":15,\"waterTemperatureNotifyMax\":35,\"waterFlowStopMin\":1800,\"waterFlowStopMax\":6500,\"waterFlowNotifyMin\":3000,\"waterFlowNotifyMax\":5700,\"electricCurrentStopMin\":170,\"electricCurrentStopMax\":250,\"electricCurrentNotifyMin\":180,\"electricCurrentNotifyMax\":240,\"electricTensionStopMin\":170,\"electricTensionStopMax\":250,\"electricTensionNotifyMin\":180,\"electricTensionNotifyMax\":240,\"technicianPassword\":\"1234\",\"secretKey\":\"thisis32bitlongpassphraseimade11\",\"tokenInitialDate\":null,\"tokenEndDate\":null,\"lastTimezone\":\"America/Sao_Paulo\"}",
                  headers: [RecordHeader(key: 'origin', value: 'machine')],
                )
              ],
            ),
          )
        ],
      ),
    ],
    async: true,
    apiVersion: 11,
    clientId: 'dart-kafka',
    producerId: -1,
    attributes: 0,
  );
}
