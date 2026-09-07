package mongo

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// The etcd revision is derived from MongoDB's clusterTime, not from a counter.
const ordinalBits = 20

// EncodeRevision converts an oplog timestamp into the etcd revision.
func EncodeRevision(ts bson.Timestamp, epochBase int64) (int64, error) {
	seg := int64(ts.T) - epochBase
	if seg < 0 {
		return 0, fmt.Errorf("clusterTime %d predates epoch base %d: the cluster epoch base is wrong", ts.T, epochBase)
	}
	if ts.I >= 1<<ordinalBits {
		return 0, fmt.Errorf("clusterTime ordinal %d exceeds %d operations within the same second", ts.I, 1<<ordinalBits)
	}
	return (seg << ordinalBits) | int64(ts.I), nil
}

// DecodeRevision goes the other way — used to resume a change stream from a
func DecodeRevision(rev int64, epochBase int64) bson.Timestamp {
	return bson.Timestamp{
		T: uint32((rev >> ordinalBits) + epochBase),
		I: uint32(rev & (1<<ordinalBits - 1)),
	}
}
