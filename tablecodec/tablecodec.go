package tablecodec

import (
	"sort"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-common/codec"
	"github.com/pingcap/tidb-common/mysql"
	"github.com/pingcap/tidb-common/tipb"
	"github.com/pingcap/tidb-common/types"
)

var (
	TablePrefix     = []byte{'t'}
	recordPrefixSep = []byte("_r")
	indexPrefixSep  = []byte("_i")
)

func EncodeRecordKey(tableId int64, h int64, columnID int64) []byte {
	recordPrefix := genTableRecordPrefix(tableId)
	buf := make([]byte, 0, len(recordPrefix)+16)
	buf = append(buf, recordPrefix...)
	buf = codec.EncodeInt(buf, h)
	if columnID != 0 {
		buf = codec.EncodeInt(buf, columnID)
	}
	return buf
}

// DecodeRecordKey decodes the key and gets the tableID, handle and columnID.
func DecodeRecordKey(key []byte) (tableID int64, handle int64, columnID int64, err error) {
	k := key
	if !key.HasPrefix(TablePrefix) {
		return 0, 0, 0, errors.Errorf("invalid record key - %q", k)
	}

	key = key[len(TablePrefix):]
	key, tableID, err = codec.DecodeInt(key)
	if err != nil {
		return 0, 0, 0, errors.Trace(err)
	}

	if !key.HasPrefix(recordPrefixSep) {
		return 0, 0, 0, errors.Errorf("invalid record key - %q", k)
	}

	key = key[len(recordPrefixSep):]

	key, handle, err = codec.DecodeInt(key)
	if err != nil {
		return 0, 0, 0, errors.Trace(err)
	}
	if len(key) == 0 {
		return
	}

	key, columnID, err = codec.DecodeInt(key)
	if err != nil {
		return 0, 0, 0, errors.Trace(err)
	}
	return
}

// DecodeValue implements table.Table DecodeValue interface.
func DecodeValue(data []byte, tp *tipb.ColumnInfo) (types.Datum, error) {
	values, err := codec.Decode(data)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return unflatten(values[0], tp)
}

func unflatten(datum types.Datum, tp *tipb.ColumnInfo) (types.Datum, error) {
	if datum.Kind() == types.KindNull {
		return datum, nil
	}
	switch tp.GetTp() {
	case tipb.MysqlType_TypeFloat:
		datum.SetFloat32(float32(datum.GetFloat64()))
		return datum, nil
	case tipb.MysqlType_TypeTiny, tipb.MysqlType_TypeShort, tipb.MysqlType_TypeYear, tipb.MysqlType_TypeInt24,
		tipb.MysqlType_TypeLong, tipb.MysqlType_TypeLonglong, tipb.MysqlType_TypeDouble, tipb.MysqlType_TypeTinyBlob,
		tipb.MysqlType_TypeMediumBlob, tipb.MysqlType_TypeBlob, tipb.MysqlType_TypeLongBlob, tipb.MysqlType_TypeVarchar,
		tipb.MysqlType_TypeString:
		return datum, nil
	case tipb.MysqlType_TypeDate, tipb.MysqlType_TypeDatetime, tipb.MysqlType_TypeTimestamp:
		var t mysql.Time
		t.Type = uint8(tp.GetTp())
		t.Fsp = int(tp.GetDecimal())
		err := t.Unmarshal(datum.GetBytes())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetValue(t)
		return datum, nil
	case tipb.MysqlType_TypeDuration:
		dur := mysql.Duration{Duration: time.Duration(datum.GetInt64())}
		datum.SetValue(dur)
		return datum, nil
	case tipb.MysqlType_TypeNewDecimal:
		dec, err := mysql.ParseDecimal(datum.GetString())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetValue(dec)
		return datum, nil
	case tipb.MysqlType_TypeEnum:
		enum, err := mysql.ParseEnumValue(tp.Elems, datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetValue(enum)
		return datum, nil
	case tipb.MysqlType_TypeSet:
		set, err := mysql.ParseSetValue(tp.Elems, datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetValue(set)
		return datum, nil
	case tipb.MysqlType_TypeBit:
		bit := mysql.Bit{Value: datum.GetUint64(), Width: int(tp.GetColumnLen())}
		datum.SetValue(bit)
		return datum, nil
	}
	log.Error(tp.GetTp(), datum)
	return datum, nil
}

func EncodeIndexKey(tableId int64, indexedValues []types.Datum, handle int64, unique bool) (key []byte, distinct bool, err error) {
	if unique {
		// See: https://dev.mysql.com/doc/refman/5.7/en/create-index.html
		// A UNIQUE index creates a constraint such that all values in the index must be distinct.
		// An error occurs if you try to add a new row with a key value that matches an existing row.
		// For all engines, a UNIQUE index permits multiple NULL values for columns that can contain NULL.
		distinct = true
		for _, cv := range indexedValues {
			if cv.Kind() == types.KindNull {
				distinct = false
				break
			}
		}
	}
	prefix := genTableIndexPrefix(tableId)
	key = append(key, prefix...)
	if distinct {
		key, err = codec.EncodeKey(key, indexedValues...)
	} else {
		key, err = codec.EncodeKey(key, append(indexedValues, types.NewDatum(handle))...)
	}
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return key, distinct, nil
}

// record prefix is "t[tableID]_r"
func genTableRecordPrefix(tableID int64) []byte {
	buf := make([]byte, 0, len(TablePrefix)+8+len(recordPrefixSep))
	buf = append(buf, TablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, recordPrefixSep...)
	return buf
}

// index prefix is "t[tableID]_i"
func genTableIndexPrefix(tableID int64) []byte {
	buf := make([]byte, 0, len(TablePrefix)+8+len(indexPrefixSep))
	buf = append(buf, TablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, indexPrefixSep...)
	return buf
}

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func SortHandles(handles []int64) {
	sort.Sort(int64Slice(handles))
}
