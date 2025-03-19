import 'dart:typed_data';

import 'package:dart_kafka/src/models/responses/init_producer_id_response.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/definitions/errors.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaInitProducerIdApi {
  final int apiKey = INIT_PRODUCER_ID;
  Utils utils = Utils();
  Encoder encoder = Encoder();

  /// Method to build and serialize the InitProduceId to Byte Array
  Uint8List serialize({
    String? transactionalId,
    required int transactionTimeoutMs,
    required int producerId,
    required int producerEpoch,
    required int apiVersion,
    required String? clientId,
    required int correlationId,
  }) {
    BytesBuilder buffer = BytesBuilder();

    if (apiVersion < 2) {
      buffer.add(encoder.nullableString(transactionalId));
    } else {
      buffer.add(encoder.compactNullableString(transactionalId));
    }

    buffer.add(encoder.int32(transactionTimeoutMs));

    if (apiVersion > 2) {
      buffer.add(encoder.int64(producerId));
      buffer.add(encoder.int16(producerEpoch));
    }

    if (apiVersion > 1) {
      buffer.add(encoder.int8(0)); // _tagged_fields
    }

    var message = buffer.toBytes();
    buffer.clear();

    Uint8List header = encoder.writeMessageHeader(
        version: apiVersion > 1 ? 2 : 1,
        messageLength: message.length,
        apiKey: apiKey,
        apiVersion: apiVersion,
        correlationId: correlationId,
        clientId: clientId);

    message = Uint8List.fromList([...header, ...message]);
    return message;
  }

  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    final throttleTimeMs = buffer.getInt32(offset, Endian.big);
    offset += 4;
    final errorCode = buffer.getInt16(offset, Endian.big);
    offset += 2;
    final producerId = buffer.getInt64(offset, Endian.big);
    offset += 8;
    final producerEpoch = buffer.getInt16(offset, Endian.big);
    offset += 2;

    int? taggedFields;
    if (apiVersion > 1) {
      taggedFields = buffer.getInt8(offset);
    }

    return InitProducerIdResponse(
        throttleTimeMs: throttleTimeMs,
        errorCode: errorCode,
        errorMessage: (ERROR_MAP[errorCode] as Map)['message'],
        producerId: producerId,
        producerEpoch: producerEpoch,
        taggedFields: taggedFields);
  }
}
