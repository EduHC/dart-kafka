// ignore_for_file: constant_identifier_names

const UNKNOWN_SERVER_ERROR = -1;
const NONE = 0;
const OFFSET_OUT_OF_RANGE = 1;
const CORRUPT_MESSAGE = 2;
const UNKNOWN_TOPIC_OR_PARTITION = 3;
const INVALID_FETCH_SIZE = 4;
const LEADER_NOT_AVAILABLE = 5;
const NOT_LEADER_OR_FOLLOWER = 6;
const REQUEST_TIMED_OUT = 7;
const BROKER_NOT_AVAILABLE = 8;
const REPLICA_NOT_AVAILABLE = 9;
const MESSAGE_TOO_LARGE = 10;
const STALE_CONTROLLER_EPOCH = 11;
const OFFSET_METADATA_TOO_LARGE = 12;
const NETWORK_EXCEPTION = 13;
const COORDINATOR_LOAD_IN_PROGRESS = 14;
const COORDINATOR_NOT_AVAILABLE = 15;
const NOT_COORDINATOR = 16;
const INVALID_TOPIC_EXCEPTION = 17;
const RECORD_LIST_TOO_LARGE = 18;
const NOT_ENOUGH_REPLICAS = 19;
const NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20;
const INVALID_REQUIRED_ACKS = 21;
const ILLEGAL_GENERATION = 22;
const INCONSISTENT_GROUP_PROTOCOL = 23;
const INVALID_GROUP_ID = 24;
const UNKNOWN_MEMBER_ID = 25;
const INVALID_SESSION_TIMEOUT = 26;
const REBALANCE_IN_PROGRESS = 27;
const INVALID_COMMIT_OFFSET_SIZE = 28;
const TOPIC_AUTHORIZATION_FAILED = 29;
const GROUP_AUTHORIZATION_FAILED = 30;
const CLUSTER_AUTHORIZATION_FAILED = 31;
const INVALID_TIMESTAMP = 32;
const UNSUPPORTED_SASL_MECHANISM = 33;
const ILLEGAL_SASL_STATE = 34;
const UNSUPPORTED_VERSION = 35;
const TOPIC_ALREADY_EXISTS = 36;
const INVALID_PARTITIONS = 37;
const INVALID_REPLICATION_FACTOR = 38;
const INVALID_REPLICA_ASSIGNMENT = 39;
const INVALID_CONFIG = 40;
const NOT_CONTROLLER = 41;
const INVALID_REQUEST = 42;
const UNSUPPORTED_FOR_MESSAGE_FORMAT = 43;
const POLICY_VIOLATION = 44;
const OUT_OF_ORDER_SEQUENCE_NUMBER = 45;
const DUPLICATE_SEQUENCE_NUMBER = 46;
const INVALID_PRODUCER_EPOCH = 47;
const INVALID_TXN_STATE = 48;
const INVALID_PRODUCER_ID_MAPPING = 49;
const INVALID_TRANSACTION_TIMEOUT = 50;
const CONCURRENT_TRANSACTIONS = 51;
const TRANSACTION_COORDINATOR_FENCED = 52;
const TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53;
const SECURITY_DISABLED = 54;
const OPERATION_NOT_ATTEMPTED = 55;
const KAFKA_STORAGE_ERROR = 56;
const LOG_DIR_NOT_FOUND = 57;
const SASL_AUTHENTICATION_FAILED = 58;
const UNKNOWN_PRODUCER_ID = 59;
const REASSIGNMENT_IN_PROGRESS = 60;
const DELEGATION_TOKEN_AUTH_DISABLED = 61;
const DELEGATION_TOKEN_NOT_FOUND = 62;
const DELEGATION_TOKEN_OWNER_MISMATCH = 63;
const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64;
const DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65;
const DELEGATION_TOKEN_EXPIRED = 66;
const INVALID_PRINCIPAL_TYPE = 67;
const NON_EMPTY_GROUP = 68;
const GROUP_ID_NOT_FOUND = 69;
const FETCH_SESSION_ID_NOT_FOUND = 70;
const INVALID_FETCH_SESSION_EPOCH = 71;
const LISTENER_NOT_FOUND = 72;
const TOPIC_DELETION_DISABLED = 73;
const FENCED_LEADER_EPOCH = 74;
const UNKNOWN_LEADER_EPOCH = 75;
const UNSUPPORTED_COMPRESSION_TYPE = 76;
const STALE_BROKER_EPOCH = 77;
const OFFSET_NOT_AVAILABLE = 78;
const MEMBER_ID_REQUIRED = 79;
const PREFERRED_LEADER_NOT_AVAILABLE = 80;
const GROUP_MAX_SIZE_REACHED = 81;
const FENCED_INSTANCE_ID = 82;
const ELIGIBLE_LEADERS_NOT_AVAILABLE = 83;
const ELECTION_NOT_NEEDED = 84;
const NO_REASSIGNMENT_IN_PROGRESS = 85;
const GROUP_SUBSCRIBED_TO_TOPIC = 86;
const INVALID_RECORD = 87;
const UNSTABLE_OFFSET_COMMIT = 88;
const THROTTLING_QUOTA_EXCEEDED = 89;
const PRODUCER_FENCED = 90;
const RESOURCE_NOT_FOUND = 91;
const DUPLICATE_RESOURCE = 92;
const UNACCEPTABLE_CREDENTIAL = 93;
const INCONSISTENT_VOTER_SET = 94;
const INVALID_UPDATE_VERSION = 95;
const FEATURE_UPDATE_FAILED = 96;
const PRINCIPAL_DESERIALIZATION_FAILURE = 97;
const SNAPSHOT_NOT_FOUND = 98;
const POSITION_OUT_OF_RANGE = 99;
const UNKNOWN_TOPIC_ID = 100;
const DUPLICATE_BROKER_REGISTRATION = 101;
const BROKER_ID_NOT_REGISTERED = 102;
const INCONSISTENT_TOPIC_ID = 103;
const INCONSISTENT_CLUSTER_ID = 104;
const TRANSACTIONAL_ID_NOT_FOUND = 105;
const FETCH_SESSION_TOPIC_ID_ERROR = 106;
const INELIGIBLE_REPLICA = 107;
const NEW_LEADER_ELECTED = 108;
const OFFSET_MOVED_TO_TIERED_STORAGE = 109;
const FENCED_MEMBER_EPOCH = 110;
const UNRELEASED_INSTANCE_ID = 111;
const UNSUPPORTED_ASSIGNOR = 112;
const STALE_MEMBER_EPOCH = 113;
const MISMATCHED_ENDPOINT_TYPE = 114;
const UNSUPPORTED_ENDPOINT_TYPE = 115;
const UNKNOWN_CONTROLLER_ID = 116;
const UNKNOWN_SUBSCRIPTION_ID = 117;
const TELEMETRY_TOO_LARGE = 118;
const INVALID_REGISTRATION = 119;
const TRANSACTION_ABORTABLE = 120;
const INVALID_RECORD_STATE = 121;
const SHARE_SESSION_NOT_FOUND = 122;
const INVALID_SHARE_SESSION_EPOCH = 123;
const FENCED_STATE_EPOCH = 124;
const INVALID_VOTER_KEY = 125;
const DUPLICATE_VOTER = 126;
const VOTER_NOT_FOUND = 127;

const ERROR_MAP = {
  -1: 'Unknown server error!',
  0: 'None',
  1: 'Offset out of range!',
  2: 'Corrupt message!',
  3: 'Unknown topic or partition!',
  4: 'Invalid fetch size!',
  5: 'Leader not available!',
  6: 'Not leader or follower!',
  7: 'Request timed out!',
  8: 'Broker not available!',
  9: 'Replica not available!',
  10: 'Message too large!',
  11: 'Stale controller epoch!',
  12: 'Offset metadata too large!',
  13: 'Network exception!',
  14: 'Coordinator load in progress!',
  15: 'Coordinator not available!',
  16: 'Not coordinator!',
  17: 'Invalid topic exception!',
  18: 'Record list too large!',
  19: 'Not enough replicas!',
  20: 'Not enough replicas after append!',
  21: 'Invalid required acks!',
  22: 'Illegal generation!',
  23: 'Inconsistent group protocol!',
  24: 'Invalid group ID!',
  25: 'Unknown member ID!',
  26: 'Invalid session timeout!',
  27: 'Rebalance in progress!',
  28: 'Invalid commit offset size!',
  29: 'Topic authorization failed!',
  30: 'Group authorization failed!',
  31: 'Cluster authorization failed!',
  32: 'Invalid timestamp!',
  33: 'Unsupported SASL mechanism!',
  34: 'Illegal SASL state!',
  35: 'Unsupported version!',
  36: 'Topic already exists!',
  37: 'Invalid partitions!',
  38: 'Invalid replication factor!',
  39: 'Invalid replica assignment!',
  40: 'Invalid config!',
  41: 'Not controller!',
  42: 'Invalid request!',
  43: 'Unsupported for message format!',
  44: 'Policy violation!',
  45: 'Out of order sequence number!',
  46: 'Duplicate sequence number!',
  47: 'Invalid producer epoch!',
  48: 'Invalid transaction state!',
  49: 'Invalid producer ID mapping!',
  50: 'Invalid transaction timeout!',
  51: 'Concurrent transactions!',
  52: 'Transaction coordinator fenced!',
  53: 'Transactional ID authorization failed!',
  54: 'Security disabled!',
  55: 'Operation not attempted!',
  56: 'Kafka storage error!',
  57: 'Log directory not found!',
  58: 'SASL authentication failed!',
  59: 'Unknown producer ID!',
  60: 'Reassignment in progress!',
  61: 'Delegation token authentication disabled!',
  62: 'Delegation token not found!',
  63: 'Delegation token owner mismatch!',
  64: 'Delegation token request not allowed!',
  65: 'Delegation token authorization failed!',
  66: 'Delegation token expired!',
  67: 'Invalid principal type!',
  68: 'Non-empty group!',
  69: 'Group ID not found!',
  70: 'Fetch session ID not found!',
  71: 'Invalid fetch session epoch!',
  72: 'Listener not found!',
  73: 'Topic deletion disabled!',
  74: 'Fenced leader epoch!',
  75: 'Unknown leader epoch!',
  76: 'Unsupported compression type!',
  77: 'Stale broker epoch!',
  78: 'Offset not available!',
  79: 'Member ID required!',
  80: 'Preferred leader not available!',
  81: 'Group max size reached!',
  82: 'Fenced instance ID!',
  83: 'Eligible leaders not available!',
  84: 'Election not needed!',
  85: 'No reassignment in progress!',
  86: 'Group subscribed to topic!',
  87: 'Invalid record!',
  88: 'Unstable offset commit!',
  89: 'Throttling quota exceeded!',
  90: 'Producer fenced!',
  91: 'Resource not found!',
  92: 'Duplicate resource!',
  93: 'Unacceptable credential!',
  94: 'Inconsistent voter set!',
  95: 'Invalid update version!',
  96: 'Feature update failed!',
  97: 'Principal deserialization failure!',
  98: 'Snapshot not found!',
  99: 'Position out of range!',
  100: 'Unknown topic ID!',
  101: 'Duplicate broker registration!',
  102: 'Broker ID not registered!',
  103: 'Inconsistent topic ID!',
  104: 'Inconsistent cluster ID!',
  105: 'Transactional ID not found!',
  106: 'Fetch session topic ID error!',
  107: 'Ineligible replica!',
  108: 'New leader elected!',
  109: 'Offset moved to tiered storage!',
  110: 'Fenced member epoch!',
  111: 'Unreleased instance ID!',
  112: 'Unsupported assignor!',
  113: 'Stale member epoch!',
  114: 'Mismatched endpoint type!',
  115: 'Unsupported endpoint type!',
  116: 'Unknown controller ID!',
  117: 'Unknown subscription ID!',
  118: 'Telemetry too large!',
  119: 'Invalid registration!',
  120: 'Transaction abortable!',
  121: 'Invalid record state!',
  122: 'Share session not found!',
  123: 'Invalid share session epoch!',
  124: 'Fenced state epoch!',
  125: 'Invalid voter key!',
  126: 'Duplicate voter!',
  127: 'Voter not found!',
};
