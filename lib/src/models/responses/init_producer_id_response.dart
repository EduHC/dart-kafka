class InitProducerIdResponse {
  final int throttleTimeMs;
  final int errorCode;
  final String errorMessage;
  final int producerId;
  final int producerEpoch;
  final int? taggedFields;

  InitProducerIdResponse({
    required this.throttleTimeMs,
    required this.errorCode,
    required this.errorMessage,
    required this.producerId,
    required this.producerEpoch,
    this.taggedFields,
  });

  @override
  String toString() {
    return "InitProducerIdResponse -> throttleTimeMs: $throttleTimeMs, errorCode: $errorCode, errorMessage: $errorMessage, "
        "producerId: $producerId, producerEpoch: $producerEpoch, taggedFields: $taggedFields";
  }
}
