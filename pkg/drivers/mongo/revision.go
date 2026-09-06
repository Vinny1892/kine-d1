package mongo

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A revisão do etcd é derivada do clusterTime do MongoDB, e não de um
// contador. Ver docs/adr/0001-revisao-por-clustertime.md.
//
// A medição que motivou a decisão (spikes/results/mspike-4.md): um documento
// contador sofre WriteConflict sob concorrência — a variante correta entregava
// 8,2 escritas/s — e, pior, o Change Stream entrega os eventos na ordem de
// commit do oplog, que NÃO corresponde à ordem de aquisição do contador. Com
// clusterTime a ordem do watch é a ordem das revisões por construção.
//
// Layout do int64:
//
//	63       52                          20                    0
//	+--------+---------------------------+---------------------+
//	| zero   | segundos desde EpochBase  | ordinal no segundo  |
//	+--------+---------------------------+---------------------+
//	                   43 bits                   20 bits
//
// 20 bits de ordinal comportam 1.048.575 operações no mesmo segundo — muito
// acima do teto de 100 ops/s do M0 e do throughput de qualquer cluster que
// caiba em 512 MB. Os 43 bits de segundos cobrem ~278 mil anos.
const ordinalBits = 20

// EncodeRevision converte um timestamp do oplog na revisão do etcd.
func EncodeRevision(ts bson.Timestamp, epochBase int64) (int64, error) {
	seg := int64(ts.T) - epochBase
	if seg < 0 {
		return 0, fmt.Errorf("clusterTime %d é anterior ao epoch base %d: o epoch base do cluster está errado", ts.T, epochBase)
	}
	if ts.I >= 1<<ordinalBits {
		return 0, fmt.Errorf("ordinal %d do clusterTime excede %d operações no mesmo segundo", ts.I, 1<<ordinalBits)
	}
	return (seg << ordinalBits) | int64(ts.I), nil
}

// DecodeRevision faz o caminho inverso — usado para retomar um Change Stream
// a partir de uma revisão, quando o resume token não está disponível.
func DecodeRevision(rev int64, epochBase int64) bson.Timestamp {
	return bson.Timestamp{
		T: uint32((rev >> ordinalBits) + epochBase),
		I: uint32(rev & (1<<ordinalBits - 1)),
	}
}
