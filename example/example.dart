import 'package:dart_kafka/src/kafka_admin.dart';
import 'package:dart_kafka/src/kafka_client.dart';

void main() async {
  KafkaClient kafka = KafkaClient(host: '192.168.4.163', port: 29092);
  await kafka.connect();
  
  if(kafka.server == null) {
    print("Socket não conectado");
    return;
  }

  KafkaAdmin admin = KafkaAdmin(kafka: kafka);
  await admin.sendApiVersionRequest();
}