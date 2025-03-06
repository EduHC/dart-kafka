import 'dart:typed_data';
import 'package:dart_kafka/src/models/components/protocol.dart';
import 'package:dart_kafka/src/models/responses/join_group_response.dart';
import 'package:dart_kafka/src/protocol/apis.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaJoinGroupApi {
  final int apiKey = JOIN_GROUP;
  final Utils utils = Utils();

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

    byteBuffer.add(utils.int16(apiKey));
    byteBuffer.add(utils.int16(apiVersion));
    byteBuffer.add(utils.int32(correlationId));

    List<int> strUnits = groupId.codeUnits;
    byteBuffer.add(utils.int16(strUnits.length));
    byteBuffer.add(strUnits);

    // byteBuffer.add(utils.compactString(groupId));
    byteBuffer.add(utils.int32(sessionTimeoutMs));
    // byteBuffer.add(utils.int32(rebalanceTimeoutMs));
    strUnits = memberId.codeUnits;
    byteBuffer.add(utils.int16(strUnits.length));
    byteBuffer.add(strUnits);

    // byteBuffer.add(utils.compactString(memberId));
    // byteBuffer.add(utils.compactNullableString(groupInstanceId));
    strUnits = protocolType.codeUnits;
    byteBuffer.add(utils.int16(strUnits.length));
    byteBuffer.add(strUnits);
    // byteBuffer.add(utils.compactString(protocolType));

    byteBuffer.add(utils.int32(protocols.length));
    for (final protocol in protocols) {
      byteBuffer.add(utils.compactString(protocol.name));

      final BytesBuilder metadataBytes = BytesBuilder();
      metadataBytes.add(utils.int16(protocol.metadata.version));
      metadataBytes.add([protocol.metadata.topics.length]);

      for (String topic in protocol.metadata.topics) {
        metadataBytes.add(topic.codeUnits);
      }
      byteBuffer.add(utils.compactBytes(metadataBytes.toBytes()));

      if (protocol.metadata.userDataBytes != null) {
        byteBuffer.add(protocol.metadata.userDataBytes!);
      }
    }

    // byteBuffer.add(utils.compactNullableString(reason));

    final message = Uint8List.fromList(
        [...utils.int32(byteBuffer.toBytes().length), ...byteBuffer.toBytes()]);

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

    final protocolType = utils.readCompactString(buffer, offset);
    offset += protocolType.bytesRead;

    final protocolName = utils.readCompactString(buffer, offset);
    offset += protocolName.bytesRead;

    final leader = utils.readCompactString(buffer, offset);
    offset += leader.bytesRead;

    final memberId = utils.readCompactString(buffer, offset);
    offset += memberId.bytesRead;

    final membersLength = utils.readCompactArrayLength(buffer, offset);
    offset += membersLength.bytesRead;

    final members = <Map<String, dynamic>>[];
    for (int i = 0; i < membersLength.value; i++) {
      final memberId = utils.readCompactString(buffer, offset);
      offset += memberId.bytesRead;

      final groupInstanceId = utils.readCompactNullableString(buffer, offset);
      offset += groupInstanceId.bytesRead;

      final metadata = utils.readCompactBytes(buffer, offset);
      offset += metadata.bytesRead;

      members.add({
        'member_id': memberId.value,
        'group_instance_id': groupInstanceId.value,
        'metadata': metadata.value,
      });
    }

    utils.readTagBuffer(buffer, offset);

    return JoinGroupResponse(
        throttleTimeMs: throttleTimeMs,
        errorCode: errorCode,
        generationId: generationId,
        leader: leader.value,
        memberId: memberId.value);
  }

}
