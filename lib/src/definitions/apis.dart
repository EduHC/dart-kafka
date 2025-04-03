// ignore_for_file: constant_identifier_names

const PRODUCE = 0;
const FETCH = 1;
const LIST_OFFSETS = 2;
const METADATA = 3;
const LEADER_AND_ISR = 4;
const STOP_REPLICA = 5;
const UPDATE_METADATA = 6;
const CONTROLLED_SHUTDOWN = 7;
const OFFSET_COMMIT = 8;
const OFFSET_FETCH = 9;
const FIND_COORDINATOR = 10;
const JOIN_GROUP = 11;
const HEARTBEAT = 12;
const LEAVE_GROUP = 13;
const SYNC_GROUP = 14;
const DESCRIBE_GROUPS = 15;
const LIST_GROUPS = 16;
const SASL_HANDSHAKE = 17;
const API_VERSIONS = 18;
const CREATE_TOPICS = 19;
const DELETE_TOPICS = 20;
const DELETE_RECORDS = 21;
const INIT_PRODUCER_ID = 22;
const OFFSET_FOR_LEADER_EPOCH = 23;
const ADD_PARTITIONS_TO_TXN = 24;
const ADD_OFFSETS_TO_TXN = 25;
const END_TXN = 26;
const WRITE_TXN_MARKERS = 27;
const TXN_OFFSET_COMMIT = 28;
const DESCRIBE_ACLS = 29;
const CREATE_ACLS = 30;
const DELETE_ACLS = 31;
const DESCRIBE_CONFIGS = 32;
const ALTER_CONFIGS = 33;
const ALTER_REPLICA_LOG_DIRS = 34;
const DESCRIBE_LOG_DIRS = 35;
const SASL_AUTHENTICATE = 36;
const CREATE_PARTITIONS = 37;
const CREATE_DELEGATION_TOKEN = 38;
const RENEW_DELEGATION_TOKEN = 39;
const EXPIRE_DELEGATION_TOKEN = 40;
const DESCRIBE_DELEGATION_TOKEN = 41;
const DELETE_GROUPS = 42;
const ELECT_LEADERS = 43;
const INCREMENTAL_ALTER_CONFIGS = 44;
const ALTER_PARTITION_REASSIGNMENTS = 45;
const LIST_PARTITION_REASSIGNMENTS = 46;
const OFFSET_DELETE = 47;
const DESCRIBE_CLIENT_QUOTAS = 48;
const ALTER_CLIENT_QUOTAS = 49;
const DESCRIBE_USER_SCRAM_CREDENTIALS = 50;
const ALTER_USER_SCRAM_CREDENTIALS = 51;
const DESCRIBE_QUORUM = 55;
const ALTER_PARTITION = 56;
const UPDATE_FEATURES = 57;
const ENVELOPE = 58;
const DESCRIBE_CLUSTER = 60;
const DESCRIBE_PRODUCERS = 61;
const UNREGISTER_BROKER = 64;
const DESCRIBE_TRANSACTIONS = 65;
const LIST_TRANSACTIONS = 66;
const ALLOCATE_PRODUCER_IDS = 67;
const CONSUMER_GROUP_HEARTBEAT = 68;
const CONSUMER_GROUP_DESCRIBE = 69;
const GET_TELEMETRY_SUBSCRIPTIONS = 71;
const PUSH_TELEMETRY = 72;
const LIST_CLIENT_METRICS_RESOURCES = 74;
const DESCRIBE_TOPIC_PARTITIONS = 75;
const ADD_RAFT_VOTER = 80;
const REMOVE_RAFT_VOTER = 81;

const API_REQUIRE_SPECIFIC_BROKER = {
  FETCH: true,
  PRODUCE: true,
  LIST_OFFSETS: true,
  DESCRIBE_PRODUCERS: true,
  METADATA: false,
  API_VERSIONS: false,
  INIT_PRODUCER_ID: false,
  JOIN_GROUP: false,
  FIND_COORDINATOR: false,
  HEARTBEAT: false,
  SYNC_GROUP: false,
};
