import 'dart:typed_data';
import 'package:dart_kafka/src/models/components/protocol.dart';
import 'package:dart_kafka/src/models/responses/join_group_response.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/protocol/decoder.dart';
import 'package:dart_kafka/src/protocol/endocer.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaJoinGroupApi {
  final int apiKey = JOIN_GROUP;
  final Utils utils = Utils();
  final Encoder encoder = Encoder();
  final Decoder decoder = Decoder();

  /// Serialize the JoinGroupRequest to bytes
  Uint8List serialize(
      {required int correlationId,
      required String groupId,
      required int sessionTimeoutMs,
      required int rebalanceTimeoutMs,
      required String memberId,
      String? groupInstanceId,
      required String protocolType,
      required List<Protocol> protocols,
      String? reason,
      required int apiVersion}) {
    final byteBuffer = BytesBuilder();

    byteBuffer.add(encoder.compactString(groupId));
    byteBuffer.add(encoder.int32(sessionTimeoutMs));
    byteBuffer.add(encoder.int32(rebalanceTimeoutMs));
    byteBuffer.add(encoder.compactString(memberId));
    byteBuffer.add(encoder.compactNullableString(groupInstanceId));
    byteBuffer.add(encoder.compactString(protocolType));
    byteBuffer.add(encoder.compactArrayLength(protocols.length));

    for (final protocol in protocols) {
      byteBuffer.add(encoder.compactString(protocol.name));

      final BytesBuilder metadataBytes = BytesBuilder();
      metadataBytes.add(encoder.int16(protocol.metadata.version));
      metadataBytes.add([protocol.metadata.topics.length]);

      for (String topic in protocol.metadata.topics) {
        metadataBytes.add(topic.codeUnits);
      }
      byteBuffer.add(encoder.compactBytes(metadataBytes.toBytes()));

      if (protocol.metadata.userDataBytes != null) {
        byteBuffer.add(protocol.metadata.userDataBytes!);
      }
    }

    // byteBuffer.add(utils.compactNullableString(reason));

    final message = Uint8List.fromList([
      ...encoder.int32(byteBuffer.toBytes().length),
      ...byteBuffer.toBytes()
    ]);

    final buffer = ByteData.sublistView(message);
    return message;
  }

  /// Deserialize the JoinGroupResponse (Version 9)
  dynamic deserialize(Uint8List data, int apiVersion) {
    final buffer = ByteData.sublistView(data);
    int offset = 0;

    final throttleTimeMs = buffer.getInt32(offset);
    offset += 4;

    final errorCode = buffer.getInt16(offset);
    offset += 2;

    final generationId = buffer.getInt32(offset);
    offset += 4;

    final protocolType = decoder.readCompactString(buffer, offset);
    offset += protocolType.bytesRead;

    final protocolName = decoder.readCompactString(buffer, offset);
    offset += protocolName.bytesRead;

    final leader = decoder.readCompactString(buffer, offset);
    offset += leader.bytesRead;

    final memberId = decoder.readCompactString(buffer, offset);
    offset += memberId.bytesRead;

    final membersLength = decoder.readCompactArrayLength(buffer, offset);
    offset += membersLength.bytesRead;

    final members = <Map<String, dynamic>>[];
    for (int i = 0; i < membersLength.value; i++) {
      final memberId = decoder.readCompactString(buffer, offset);
      offset += memberId.bytesRead;

      final groupInstanceId = decoder.readCompactNullableString(buffer, offset);
      offset += groupInstanceId.bytesRead;

      final metadata = decoder.readCompactBytes(buffer, offset);
      offset += metadata.bytesRead;

      members.add({
        'member_id': memberId.value,
        'group_instance_id': groupInstanceId.value,
        'metadata': metadata.value,
      });
    }

    decoder.readTagBuffer(buffer, offset);

    return JoinGroupResponse(
        throttleTimeMs: throttleTimeMs,
        errorCode: errorCode,
        generationId: generationId,
        leader: leader.value,
        memberId: memberId.value);
  }
}
