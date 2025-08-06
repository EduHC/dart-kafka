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
  -1: {
    'message':
        'The server experienced an unexpected error when processing the request.',
    'retry': false,
  },
  0: {'message': null, 'retry': false},
  1: {
    'message':
        'The requested offset is not within the range of offsets maintained by the server.',
    'retry': false,
  },
  2: {
    'message':
        'This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.',
    'retry': false,
  },
  3: {
    'message': 'This server does not host this topic-partition.',
    'retry': false,
  },
  4: {'message': 'The requested fetch size is invalid.', 'retry': false},
  5: {
    'message':
        'There is no leader for this topic-partition as we are in the middle of a leadership election.',
    'retry': true,
  },
  6: {
    'message':
        'For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.',
    'retry': true,
  },
  7: {'message': 'The request timed out.', 'retry': true},
  8: {'message': 'The broker is not available.', 'retry': true},
  9: {
    'message':
        'The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.',
    'retry': true,
  },
  10: {
    'message':
        'The request included a message larger than the max message size the server will accept.',
    'retry': false,
  },
  11: {'message': 'The controller moved to another broker.', 'retry': true},
  12: {
    'message': 'The metadata field of the offset request was too large.',
    'retry': false,
  },
  13: {
    'message': 'The server disconnected before a response was received.',
    'retry': true,
  },
  14: {
    'message': 'The coordinator is loading and hence can\'t process requests.',
    'retry': true,
  },
  15: {'message': 'The coordinator is not available.', 'retry': true},
  16: {'message': 'This is not the correct coordinator.', 'retry': true},
  17: {
    'message':
        'The request attempted to perform an operation on an invalid topic.',
    'retry': false,
  },
  18: {
    'message':
        'The request included message batch larger than the configured segment size on the server.',
    'retry': false,
  },
  19: {
    'message':
        'Messages are rejected since there are fewer in-sync replicas than required.',
    'retry': true,
  },
  20: {
    'message':
        'Messages are written to the log, but to fewer in-sync replicas than required.',
    'retry': true,
  },
  21: {
    'message': 'Produce request specified an invalid value for required acks.',
    'retry': false,
  },
  22: {
    'message': 'Specified group generation id is not valid.',
    'retry': false,
  },
  23: {
    'message':
        'The group member\'s supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.',
    'retry': false,
  },
  24: {'message': 'The configured groupId is invalid.', 'retry': false},
  25: {
    'message': 'The coordinator is not aware of this member.',
    'retry': false,
  },
  26: {
    'message':
        'The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).',
    'retry': false,
  },
  27: {
    'message': 'The group is rebalancing, so a rejoin is needed.',
    'retry': true,
  },
  28: {
    'message': 'The committing offset data size is not valid.',
    'retry': false,
  },
  29: {'message': 'Topic authorization failed.', 'retry': false},
  30: {'message': 'Group authorization failed.', 'retry': false},
  31: {'message': 'Cluster authorization failed.', 'retry': false},
  32: {
    'message': 'The timestamp of the message is out of acceptable range.',
    'retry': false,
  },
  33: {
    'message': 'The broker does not support the requested SASL mechanism.',
    'retry': false,
  },
  34: {
    'message': 'Request is not valid given the current SASL state.',
    'retry': false,
  },
  35: {'message': 'The version of API is not supported.', 'retry': false},
  36: {'message': 'Topic with this name already exists.', 'retry': false},
  37: {'message': 'Number of partitions is below 1.', 'retry': false},
  38: {
    'message':
        'Replication factor is below 1 or larger than the number of available brokers.',
    'retry': false,
  },
  39: {'message': 'Replica assignment is invalid.', 'retry': false},
  40: {'message': 'Configuration is invalid.', 'retry': false},
  41: {
    'message': 'This is not the correct controller for this cluster.',
    'retry': true,
  },
  42: {
    'message':
        'This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.',
    'retry': false,
  },
  43: {
    'message':
        'The message format version on the broker does not support the request.',
    'retry': false,
  },
  44: {
    'message': 'Request parameters do not satisfy the configured policy.',
    'retry': false,
  },
  45: {
    'message': 'The broker received an out of order sequence number.',
    'retry': false,
  },
  46: {
    'message': 'The broker received a duplicate sequence number.',
    'retry': false,
  },
  47: {
    'message': 'Producer attempted to produce with an old epoch.',
    'retry': false,
  },
  48: {
    'message':
        'The producer attempted a transactional operation in an invalid state.',
    'retry': false,
  },
  49: {
    'message':
        'The producer attempted to use a producer id which is not currently assigned to its transactional id.',
    'retry': false,
  },
  50: {
    'message':
        'The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).',
    'retry': false,
  },
  51: {
    'message':
        'The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.',
    'retry': true,
  },
  52: {
    'message':
        'Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.',
    'retry': false,
  },
  53: {'message': 'Transactional Id authorization failed.', 'retry': false},
  54: {'message': 'Security features are disabled.', 'retry': false},
  55: {
    'message':
        'The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.',
    'retry': false,
  },
  56: {
    'message': 'Disk error when trying to access log file on the disk.',
    'retry': true,
  },
  57: {
    'message':
        'The user-specified log directory is not found in the broker config.',
    'retry': false,
  },
  58: {'message': 'SASL Authentication failed.', 'retry': false},
  59: {
    'message':
        'This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer\'s records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer\'s metadata is removed from the broker, and future appends by the producer will return this exception.',
    'retry': false,
  },
  60: {'message': 'A partition reassignment is in progress.', 'retry': true},
  61: {'message': 'Delegation Token feature is not enabled.', 'retry': false},
  62: {'message': 'Delegation Token is not found on server.', 'retry': false},
  63: {
    'message': 'Specified Principal is not valid Owner/Renewer.',
    'retry': false,
  },
  64: {
    'message':
        'Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.',
    'retry': false,
  },
  65: {'message': 'Delegation Token authorization failed.', 'retry': false},
  66: {'message': 'Delegation Token is expired.', 'retry': false},
  67: {'message': 'Supplied principalType is not supported.', 'retry': false},
  68: {'message': 'The group is not empty.', 'retry': false},
  69: {'message': 'The group id does not exist.', 'retry': false},
  70: {'message': 'The fetch session ID was not found.', 'retry': false},
  71: {'message': 'The fetch session epoch is invalid.', 'retry': false},
  72: {
    'message':
        'There is no listener on the leader broker that matches the listener on which metadata request was processed.',
    'retry': false,
  },
  73: {'message': 'Topic deletion is disabled.', 'retry': false},
  74: {
    'message':
        'The leader epoch in the request is older than the epoch on the broker.',
    'retry': false,
  },
  75: {
    'message':
        'The leader epoch in the request is newer than the epoch on the broker.',
    'retry': true,
  },
  76: {
    'message':
        'The requesting client does not support the compression type of given partition.',
    'retry': false,
  },
  77: {'message': 'Broker epoch has changed.', 'retry': true},
  78: {
    'message':
        'The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.',
    'retry': false,
  },
  79: {
    'message':
        'The group member needs to have a valid member id before actually entering a consumer group.',
    'retry': false,
  },
  80: {'message': 'The preferred leader was not available.', 'retry': true},
  81: {
    'message': 'The consumer group has reached its max size.',
    'retry': false,
  },
  82: {
    'message':
        'The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.',
    'retry': false,
  },
  83: {
    'message': 'Eligible topic partition leaders are not available.',
    'retry': true,
  },
  84: {
    'message': 'Leader election not needed for topic partition.',
    'retry': false,
  },
  85: {'message': 'No partition reassignment is in progress.', 'retry': false},
  86: {
    'message':
        'Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.',
    'retry': false,
  },
  87: {
    'message':
        'This record has failed the validation on broker and hence will be rejected.',
    'retry': false,
  },
  88: {
    'message': 'There are unstable offsets that need to be cleared.',
    'retry': true,
  },
  89: {'message': 'The throttling quota has been exceeded.', 'retry': true},
  90: {
    'message':
        'There is a newer producer with the same transactionalId which fences the current one.',
    'retry': false,
  },
  91: {
    'message':
        'A request illegally referred to a resource that does not exist.',
    'retry': false,
  },
  92: {
    'message': 'A request illegally referred to the same resource twice.',
    'retry': false,
  },
  93: {
    'message':
        'Requested credential would not meet criteria for acceptability.',
    'retry': false,
  },
  94: {
    'message':
        'Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters.',
    'retry': false,
  },
  95: {'message': 'The given update version was invalid.', 'retry': false},
  96: {
    'message':
        'Unable to update finalized features due to an unexpected server error.',
    'retry': false,
  },
  97: {
    'message':
        'Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.',
    'retry': false,
  },
  98: {'message': 'Requested snapshot was not found.', 'retry': false},
  99: {
    'message':
        'Requested position is not greater than or equal to zero, and less than the size of the snapshot.',
    'retry': false,
  },
  100: {'message': 'This server does not host this topic ID.', 'retry': false},
  101: {'message': 'This broker ID is already in use.', 'retry': false},
  102: {'message': 'The given broker ID was not registered.', 'retry': false},
  103: {
    'message': 'The log\'s topic ID did not match the topic ID in the request.',
    'retry': false,
  },
  104: {
    'message':
        'The clusterId in the request does not match that found on the server.',
    'retry': false,
  },
  105: {'message': 'The transactionalId could not be found.', 'retry': false},
  106: {
    'message': 'The fetch session encountered inconsistent topic ID usage.',
    'retry': false,
  },
  107: {
    'message': 'The new ISR contains at least one ineligible replica.',
    'retry': false,
  },
  108: {
    'message':
        'The AlterPartition request successfully updated the partition state but the leader has changed.',
    'retry': true,
  },
  109: {
    'message': 'The requested offset is moved to tiered storage.',
    'retry': false,
  },
  110: {
    'message':
        'The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.',
    'retry': false,
  },
  111: {
    'message':
        'The instance ID is still used by another member in the consumer group. That member must leave first.',
    'retry': false,
  },
  112: {
    'message':
        'The assignor or its version range is not supported by the consumer group.',
    'retry': false,
  },
  113: {
    'message':
        'The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.',
    'retry': false,
  },
  114: {
    'message': 'The request was sent to an endpoint of the wrong type.',
    'retry': false,
  },
  115: {'message': 'This endpoint type is not supported yet.', 'retry': false},
  116: {'message': 'This controller ID is not known.', 'retry': false},
  117: {
    'message':
        'Client sent a push telemetry request with an invalid or outdated subscription ID.',
    'retry': false,
  },
  118: {
    'message':
        'Client sent a push telemetry request larger than the maximum size the broker will accept.',
    'retry': false,
  },
  119: {
    'message':
        'The controller has considered the broker registration to be invalid.',
    'retry': false,
  },
  120: {
    'message':
        'The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.',
    'retry': false,
  },
  121: {
    'message':
        'The record state is invalid. The acknowledgement of delivery could not be completed.',
    'retry': false,
  },
  122: {'message': 'The share session was not found.', 'retry': false},
  123: {'message': 'The share session epoch is invalid.', 'retry': false},
  124: {
    'message':
        'The share coordinator rejected the request because the share-group state epoch did not match.',
    'retry': false,
  },
  125: {
    'message': 'The voter key doesn\'t match the receiving replica\'s key.',
    'retry': false,
  },
  126: {
    'message': 'The voter is already part of the set of voters.',
    'retry': false,
  },
  127: {
    'message': 'The voter is not part of the set of voters.',
    'retry': false,
  },
};
