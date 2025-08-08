import 'api.dart';

class MessageHeaderVersion {
  static int requestHeaderVersion({
    required int apiVersion,
    required int apiKey,
  }) {
    switch (apiKey) {
      case PRODUCE:
        if (apiVersion >= 9) return 2;
        return 1;
      case FETCH:
        if (apiVersion >= 12) return 2;
        return 1;
      case LIST_OFFSETS:
        if (apiVersion >= 6) return 2;
        return 1;
      case METADATA:
        if (apiVersion >= 9) return 2;
        return 1;
      case OFFSET_COMMIT:
        if (apiVersion >= 8) return 2;
        return 1;
      case OFFSET_FETCH:
        if (apiVersion >= 6) return 2;
        return 1;
      case FIND_COORDINATOR:
        if (apiVersion >= 3) return 2;
        return 1;
      case JOIN_GROUP:
        if (apiVersion >= 6) return 2;
        return 1;
      case HEARTBEAT:
        if (apiVersion >= 4) return 2;
        return 1;
      case LEAVE_GROUP:
        if (apiVersion >= 4) return 2;
        return 1;
      case SYNC_GROUP:
        if (apiVersion >= 4) return 2;
        return 1;
      case DESCRIBE_GROUPS:
        if (apiVersion >= 5) return 2;
        return 1;
      case LIST_GROUPS:
        if (apiVersion >= 3) return 2;
        return 1;
      case SASL_HANDSHAKE:
        return 1;
      case API_VERSIONS:
        if (apiVersion >= 3) return 2;
        return 1;
      case CREATE_TOPICS:
        if (apiVersion >= 5) return 2;
        return 1;
      case DELETE_TOPICS:
        if (apiVersion >= 4) return 2;
        return 1;
      case DELETE_RECORDS:
        if (apiVersion >= 2) return 2;
        return 1;
      case INIT_PRODUCER_ID:
        if (apiVersion >= 2) return 2;
        return 1;
      case OFFSET_FOR_LEADER_EPOCH:
        if (apiVersion >= 4) return 2;
        return 1;
      case ADD_PARTITIONS_TO_TXN:
        if (apiVersion >= 3) return 2;
        return 1;
      case ADD_OFFSETS_TO_TXN:
        if (apiVersion >= 3) return 2;
        return 1;
      case END_TXN:
        if (apiVersion >= 3) return 2;
        return 1;
      case WRITE_TXN_MARKERS:
        return 2;
      case TXN_OFFSET_COMMIT:
        if (apiVersion >= 3) return 2;
        return 1;
      case DESCRIBE_ACLS:
        if (apiVersion >= 2) return 2;
        return 1;
      case CREATE_ACLS:
        if (apiVersion >= 2) return 2;
        return 1;
      case DELETE_ACLS:
        if (apiVersion >= 2) return 2;
        return 1;
      case DESCRIBE_CONFIGS:
        if (apiVersion >= 4) return 2;
        return 1;
      case ALTER_CONFIGS:
        if (apiVersion >= 2) return 2;
        return 1;
      case ALTER_REPLICA_LOG_DIRS:
        if (apiVersion >= 2) return 2;
        return 1;
      case DESCRIBE_LOG_DIRS:
        if (apiVersion >= 2) return 2;
        return 1;
      case SASL_AUTHENTICATE:
        if (apiVersion >= 2) return 2;
        return 1;
      case CREATE_PARTITIONS:
        if (apiVersion >= 2) return 2;
        return 1;
      case CREATE_DELEGATION_TOKEN:
        if (apiVersion >= 2) return 2;
        return 1;
      case RENEW_DELEGATION_TOKEN:
        if (apiVersion >= 2) return 2;
        return 1;
      case EXPIRE_DELEGATION_TOKEN:
        if (apiVersion >= 2) return 2;
        return 1;
      case DESCRIBE_DELEGATION_TOKEN:
        if (apiVersion >= 2) return 2;
        return 1;
      case DELETE_GROUPS:
        if (apiVersion >= 2) return 2;
        return 1;
      case ELECT_LEADERS:
        if (apiVersion >= 2) return 2;
        return 1;
      case INCREMENTAL_ALTER_CONFIGS:
        if (apiVersion >= 1) return 2;
        return 1;
      case ALTER_PARTITION_REASSIGNMENTS:
        return 2;
      case LIST_PARTITION_REASSIGNMENTS:
        return 2;
      case OFFSET_DELETE:
        return 1;
      case DESCRIBE_CLIENT_QUOTAS:
        if (apiVersion >= 1) return 2;
        return 1;
      case ALTER_CLIENT_QUOTAS:
        if (apiVersion >= 1) return 2;
        return 1;
      case DESCRIBE_USER_SCRAM_CREDENTIALS:
        return 2;
      case ALTER_USER_SCRAM_CREDENTIALS:
        return 2;
      case DESCRIBE_QUORUM:
        return 2;
      case ALTER_PARTITION:
        return 2;
      case UPDATE_FEATURES:
        return 2;
      case ENVELOPE:
        return 2;
      case DESCRIBE_CLUSTER:
        return 2;
      case DESCRIBE_PRODUCERS:
        return 2;
      case UNREGISTER_BROKER:
        return 2;
      case DESCRIBE_TRANSACTIONS:
        return 2;
      case LIST_TRANSACTIONS:
        return 2;
      case ALLOCATE_PRODUCER_IDS:
        return 2;
      case CONSUMER_GROUP_HEARTBEAT:
        return 2;
      case CONSUMER_GROUP_DESCRIBE:
        return 2;
      case GET_TELEMETRY_SUBSCRIPTIONS:
        return 2;
      case PUSH_TELEMETRY:
        return 2;
      case LIST_CLIENT_METRICS_RESOURCES:
        return 2;
      case DESCRIBE_TOPIC_PARTITIONS:
        return 2;
      case ADD_RAFT_VOTER:
        return 2;
      case REMOVE_RAFT_VOTER:
        return 2;
      default:
        throw UnsupportedError('Unsupported API key $apiKey');
    }
  }

  static int responseHeaderVersion({
    required int apiVersion,
    required int apiKey,
  }) {
    switch (apiKey) {
      case PRODUCE:
        if (apiVersion >= 9) return 1;
        return 0;
      case FETCH:
        if (apiVersion >= 12) return 1;
        return 0;
      case LIST_OFFSETS:
        if (apiVersion >= 6) return 1;
        return 0;
      case METADATA:
        if (apiVersion >= 9) return 1;
        return 0;
      case OFFSET_COMMIT:
        if (apiVersion >= 8) return 1;
        return 0;
      case OFFSET_FETCH:
        if (apiVersion >= 6) return 1;
        return 0;
      case FIND_COORDINATOR:
        if (apiVersion >= 3) return 1;
        return 0;
      case JOIN_GROUP:
        if (apiVersion >= 6) return 1;
        return 0;
      case HEARTBEAT:
        if (apiVersion >= 4) return 1;
        return 0;
      case LEAVE_GROUP:
        if (apiVersion >= 4) return 1;
        return 0;
      case SYNC_GROUP:
        if (apiVersion >= 4) return 1;
        return 0;
      case DESCRIBE_GROUPS:
        if (apiVersion >= 5) return 1;
        return 0;
      case LIST_GROUPS:
        if (apiVersion >= 3) return 1;
        return 0;
      case SASL_HANDSHAKE:
        return 0;
      case API_VERSIONS:
        return 0; // ApiVersionsResponse always includes a v0 header.
      case CREATE_TOPICS:
        if (apiVersion >= 5) return 1;
        return 0;
      case DELETE_TOPICS:
        if (apiVersion >= 4) return 1;
        return 0;
      case DELETE_RECORDS:
        if (apiVersion >= 2) return 1;
        return 0;
      case INIT_PRODUCER_ID:
        if (apiVersion >= 2) return 1;
        return 0;
      case OFFSET_FOR_LEADER_EPOCH:
        if (apiVersion >= 4) return 1;
        return 0;
      case ADD_PARTITIONS_TO_TXN:
        if (apiVersion >= 3) return 1;
        return 0;
      case ADD_OFFSETS_TO_TXN:
        if (apiVersion >= 3) return 1;
        return 0;
      case END_TXN:
        if (apiVersion >= 3) return 1;
        return 0;
      case WRITE_TXN_MARKERS:
        return 1;
      case TXN_OFFSET_COMMIT:
        if (apiVersion >= 3) return 1;
        return 0;
      case DESCRIBE_ACLS:
        if (apiVersion >= 2) return 1;
        return 0;
      case CREATE_ACLS:
        if (apiVersion >= 2) return 1;
        return 0;
      case DELETE_ACLS:
        if (apiVersion >= 2) return 1;
        return 0;
      case DESCRIBE_CONFIGS:
        if (apiVersion >= 4) return 1;
        return 0;
      case ALTER_CONFIGS:
        if (apiVersion >= 2) return 1;
        return 0;
      case ALTER_REPLICA_LOG_DIRS:
        if (apiVersion >= 2) return 1;
        return 0;
      case DESCRIBE_LOG_DIRS:
        if (apiVersion >= 2) return 1;
        return 0;
      case SASL_AUTHENTICATE:
        if (apiVersion >= 2) return 1;
        return 0;
      case CREATE_PARTITIONS:
        if (apiVersion >= 2) return 1;
        return 0;
      case CREATE_DELEGATION_TOKEN:
        if (apiVersion >= 2) return 1;
        return 0;
      case RENEW_DELEGATION_TOKEN:
        if (apiVersion >= 2) return 1;
        return 0;
      case EXPIRE_DELEGATION_TOKEN:
        if (apiVersion >= 2) return 1;
        return 0;
      case DESCRIBE_DELEGATION_TOKEN:
        if (apiVersion >= 2) return 1;
        return 0;
      case DELETE_GROUPS:
        if (apiVersion >= 2) return 1;
        return 0;
      case ELECT_LEADERS:
        if (apiVersion >= 2) return 1;
        return 0;
      case INCREMENTAL_ALTER_CONFIGS:
        if (apiVersion >= 1) return 1;
        return 0;
      case ALTER_PARTITION_REASSIGNMENTS:
        return 1;
      case LIST_PARTITION_REASSIGNMENTS:
        return 1;
      case OFFSET_DELETE:
        return 0;
      case DESCRIBE_CLIENT_QUOTAS:
        if (apiVersion >= 1) return 1;
        return 0;
      case ALTER_CLIENT_QUOTAS:
        if (apiVersion >= 1) return 1;
        return 0;
      case DESCRIBE_USER_SCRAM_CREDENTIALS:
        return 1;
      case ALTER_USER_SCRAM_CREDENTIALS:
        return 1;
      case DESCRIBE_QUORUM:
        return 1;
      case ALTER_PARTITION:
        return 1;
      case UPDATE_FEATURES:
        return 1;
      case ENVELOPE:
        return 1;
      case DESCRIBE_CLUSTER:
        return 1;
      case DESCRIBE_PRODUCERS:
        return 1;
      case UNREGISTER_BROKER:
        return 1;
      case DESCRIBE_TRANSACTIONS:
        return 1;
      case LIST_TRANSACTIONS:
        return 1;
      case ALLOCATE_PRODUCER_IDS:
        return 1;
      case CONSUMER_GROUP_HEARTBEAT:
        return 1;
      case CONSUMER_GROUP_DESCRIBE:
        return 1;
      case GET_TELEMETRY_SUBSCRIPTIONS:
        return 1;
      case PUSH_TELEMETRY:
        return 1;
      case LIST_CLIENT_METRICS_RESOURCES:
        return 1;
      case DESCRIBE_TOPIC_PARTITIONS:
        return 1;
      case ADD_RAFT_VOTER:
        return 1;
      case REMOVE_RAFT_VOTER:
        return 1;
      default:
        throw UnsupportedError('Unsupported API key $apiKey');
    }
  }
}
