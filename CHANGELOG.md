## 1.0.2

- Ajudsted the ListOffsetRequest to accept the a DateTime optional parameter
    - The default value, if parameter isn't passed, is the UNIX timestamp.
- Corrected the "deserialize" method for this API to start the read at offset 0 instead of 1.
- Corrected the method in the class KafkaConsumer because it was sending multiple times all the topics informed
    - Altered to send a topic per request.
- Added the toMap()/toJson() and factories fromMap()/fromJson() in the ListOffsetResponse entity.
