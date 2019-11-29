package dbhelp

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)
const (
	DB_NVARCHAR string="NVARCHAR"
	DB_BIGINT string="BIGINT"
	DB_INT string="INT"
	DB_FLOAT string="FLOAT"
	DB_DECIMAL string="DECIMAL"
	DB_VARCHAR string="VARCHAR"
	DB_CHAR string="CHAR"
	DB_NCHAR string="NCHAR"
	DB_DATETIME string="DATETIME"
	DB_DATETIME2 string="DATETIME2"
	DB_BIT string ="BIT"
	DB_TEXT string="TEXT"
	DB_BINARY string="BINARY"
)
var (
	c_EMPTY_STRING       string
	c_BOOL_DEFAULT       bool
	c_BYTE_DEFAULT       byte
	c_COMPLEX64_DEFAULT  complex64
	c_COMPLEX128_DEFAULT complex128
	c_FLOAT32_DEFAULT    float32
	c_FLOAT64_DEFAULT    float64
	c_INT64_DEFAULT      int64
	c_UINT64_DEFAULT     uint64
	c_INT32_DEFAULT      int32
	c_UINT32_DEFAULT     uint32
	c_INT16_DEFAULT      int16
	c_UINT16_DEFAULT     uint16
	c_INT8_DEFAULT       int8
	c_UINT8_DEFAULT      uint8
	c_INT_DEFAULT        int
	c_UINT_DEFAULT       uint
	c_TIME_DEFAULT       time.Time
)

var (
	IntType   = reflect.TypeOf(c_INT_DEFAULT)
	Int8Type  = reflect.TypeOf(c_INT8_DEFAULT)
	Int16Type = reflect.TypeOf(c_INT16_DEFAULT)
	Int32Type = reflect.TypeOf(c_INT32_DEFAULT)
	Int64Type = reflect.TypeOf(c_INT64_DEFAULT)

	UintType   = reflect.TypeOf(c_UINT_DEFAULT)
	Uint8Type  = reflect.TypeOf(c_UINT8_DEFAULT)
	Uint16Type = reflect.TypeOf(c_UINT16_DEFAULT)
	Uint32Type = reflect.TypeOf(c_UINT32_DEFAULT)
	Uint64Type = reflect.TypeOf(c_UINT64_DEFAULT)

	Float32Type = reflect.TypeOf(c_FLOAT32_DEFAULT)
	Float64Type = reflect.TypeOf(c_FLOAT64_DEFAULT)

	Complex64Type  = reflect.TypeOf(c_COMPLEX64_DEFAULT)
	Complex128Type = reflect.TypeOf(c_COMPLEX128_DEFAULT)

	StringType = reflect.TypeOf(c_EMPTY_STRING)
	BoolType   = reflect.TypeOf(c_BOOL_DEFAULT)
	ByteType   = reflect.TypeOf(c_BYTE_DEFAULT)
	BytesType  = reflect.SliceOf(ByteType)

	TimeType = reflect.TypeOf(c_TIME_DEFAULT)
)

func value2String(rawValue *reflect.Value) (str string, err error) {
	aa := reflect.TypeOf((*rawValue).Interface())
	vv := reflect.ValueOf((*rawValue).Interface())
	switch aa.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		str = strconv.FormatInt(vv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		str = strconv.FormatUint(vv.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		str = strconv.FormatFloat(vv.Float(), 'f', -1, 64)
	case reflect.String:
		str = vv.String()
	case reflect.Array, reflect.Slice:
		switch aa.Elem().Kind() {
		case reflect.Uint8:
			data := rawValue.Interface().([]byte)
			str = string(data)
			if str == "\x00" {
				str = "0"
			}
		default:
			err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
		}
	// time type
	case reflect.Struct:
		if aa.ConvertibleTo(TimeType) {
			str = vv.Convert(TimeType).Interface().(time.Time).Format(time.RFC3339Nano)
		} else {
			err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
		}
	case reflect.Bool:
		str = strconv.FormatBool(vv.Bool())
	case reflect.Complex128, reflect.Complex64:
		str = fmt.Sprintf("%v", vv.Complex())
	/* TODO: unsupported types below
	   case reflect.Map:
	   case reflect.Ptr:
	   case reflect.Uintptr:
	   case reflect.UnsafePointer:
	   case reflect.Chan, reflect.Func, reflect.Interface:
	*/
	default:
		err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
	}
	return
}
func value2Bytes(rawValue *reflect.Value) ([]byte, error) {
	str, err := value2String(rawValue)
	if err != nil {
		return nil, err
	}
	return []byte(str), nil
}
func Rows2maps(rows *sql.Rows) (resultsSlice []map[string][]byte, err error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		result, err := row2map(rows, fields)
		if err != nil {
			return nil, err
		}
		resultsSlice = append(resultsSlice, result)
	}

	return resultsSlice, nil
}

func row2map(rows *sql.Rows, fields []string) (resultsMap map[string][]byte, err error) {
	result := make(map[string][]byte)
	scanResultContainers := make([]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		var scanResultContainer interface{}
		scanResultContainers[i] = &scanResultContainer
	}
	if err := rows.Scan(scanResultContainers...); err != nil {
		return nil, err
	}

	for ii, key := range fields {
		rawValue := reflect.Indirect(reflect.ValueOf(scanResultContainers[ii]))
		//if row is null then ignore
		if rawValue.Interface() == nil {
			result[key] = []byte{}
			continue
		}

		if data, err := value2Bytes(&rawValue); err == nil {
			result[key] = data
		} else {
			return nil, err // !nashtsai! REVIEW, should return err or just error log?
		}
	}
	return result, nil
}

func Rows2Strings(rows *sql.Rows) (resultsSlice []map[string]string, err error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		result, err := row2mapStr(rows, fields)
		if err != nil {
			return nil, err
		}
		resultsSlice = append(resultsSlice, result)
	}

	return resultsSlice, nil
}

func row2mapStr(rows *sql.Rows, fields []string) (resultsMap map[string]string, err error) {
	result := make(map[string]string)
	scanResultContainers := make([]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		var scanResultContainer interface{}
		scanResultContainers[i] = &scanResultContainer
	}
	if err := rows.Scan(scanResultContainers...); err != nil {
		return nil, err
	}

	for ii, key := range fields {
		rawValue := reflect.Indirect(reflect.ValueOf(scanResultContainers[ii]))
		// if row is null then as empty string
		if rawValue.Interface() == nil {
			result[key] = ""
			continue
		}

		if data, err := value2String(&rawValue); err == nil {
			result[key] = data
		} else {
			return nil, err
		}
	}
	return result, nil
}
//把Rows多行转成一个[]*struct{}
func RowsToArrStructKeepNull(rows *sql.Rows, destArr interface{}) error {
	//是特别注意数据库字断为null其结构体类型要用 NullString,NullInt等结构,除非数据库无null值
	//非指针和空对象不可转化
	rv := reflect.ValueOf(destArr)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("非对象不可转化,当前对像类型:"+reflect.TypeOf(destArr).String())
	}
	//非数组对象不可转化
	recordsValue := rv.Elem()
	if recordsValue.Kind() != reflect.Slice {
		return errors.New("请传了数组对象")
	}
	var itemType reflect.Type
	if recordsValue.Type().Elem().Kind() == reflect.Ptr {
		itemType = recordsValue.Type().Elem().Elem()
	} else {
		itemType = recordsValue.Type().Elem()
	}
	//if itemType.Kind() != reflect.Ptr {
	//	return errors.New("数组子对象不是结构指针")
	//}
	zeroValue := reflect.Value{}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	for rows.Next() {
		//跟据反射创建出实例
		pointerOfElement := reflect.New(itemType)
		//开始写入到子对象
		structValue := reflect.Indirect(pointerOfElement)
		arrayOfResults := []interface{}{}
		for _, column := range columns {
			field := structValue.FieldByName(column)
			if field == zeroValue {
				pointer := reflect.New(reflect.TypeOf([]byte{}))
				pointer.Elem().Set(reflect.ValueOf([]byte{}))
				arrayOfResults = append(arrayOfResults, pointer.Interface())
			} else {
				if !field.CanSet() {
					pointer := reflect.New(reflect.TypeOf([]byte{}))
					pointer.Elem().Set(reflect.ValueOf([]byte{}))
					arrayOfResults = append(arrayOfResults, pointer.Interface())
				} else {
					arrayOfResults = append(arrayOfResults, field.Addr().Interface())
				}
			}
		}
		err := rows.Scan(arrayOfResults...)
		if err != nil {
			return  err
		}
		//添加切片
		recordsValue = reflect.Append(recordsValue, pointerOfElement)

	}
	rv.Elem().Set(recordsValue)
	return  nil
}

func RowsToArrStructkillNull(rows *sql.Rows, destArr interface{}) error {
	//非指针和空对象不可转化
	rv := reflect.ValueOf(destArr)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("非对象不可转化,当前对像类型:"+reflect.TypeOf(destArr).String())
	}
	//非数组对象不可转化
	recordsValue := rv.Elem()
	if recordsValue.Kind() != reflect.Slice {
		return errors.New("请传了数组对象")
	}
	var itemType reflect.Type
	if recordsValue.Type().Elem().Kind() == reflect.Ptr {
		itemType = recordsValue.Type().Elem().Elem()
	} else {
		itemType = recordsValue.Type().Elem()
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	columnTypes, err  :=  rows.ColumnTypes()
	if err != nil {
		return err
	}
	//跟据字段类型构造接收器
	arrayOfResults := []interface{}{}
	arrayofcolumnType := []string{}
	for iIndex, _ := range columns {
		columnType :=  columnTypes[iIndex]
		switch columnType.DatabaseTypeName() {
		case DB_NVARCHAR,DB_VARCHAR,DB_CHAR,DB_NCHAR,DB_TEXT:
			lNullString := &sql.NullString{}
			arrayOfResults = append(arrayOfResults,lNullString)
			arrayofcolumnType  = append(arrayofcolumnType,"NullString")
		case DB_INT,DB_BIGINT:
			lNullInt64  := &sql.NullInt64{}
			arrayOfResults = append(arrayOfResults,lNullInt64)
			arrayofcolumnType  = append(arrayofcolumnType,"NullInt64")
		case DB_BIT:
			lNullBool := &sql.NullBool{}
			arrayOfResults = append(arrayOfResults,lNullBool)
			arrayofcolumnType  = append(arrayofcolumnType,"lNullBool")
		case DB_FLOAT,DB_DECIMAL:
			lNullFloat64 := &sql.NullFloat64{}
			arrayOfResults = append(arrayOfResults,lNullFloat64)
			arrayofcolumnType  = append(arrayofcolumnType,"NullFloat64")
		case DB_DATETIME,DB_DATETIME2:
			lNullTime  := &sql.NullTime{}
			arrayOfResults = append(arrayOfResults,lNullTime)
			arrayofcolumnType  = append(arrayofcolumnType,"NullTime")
		case DB_BINARY:
			lbyte := []byte{}
			arrayOfResults = append(arrayOfResults,&lbyte)
			arrayofcolumnType  = append(arrayofcolumnType,"byte")
		default:
			return errors.New("未定义的类型,请完善->"+columnType.DatabaseTypeName())
		}

	}
	zeroValue := reflect.Value{}
	for rows.Next() {
		//扫描到一个数组
		err := rows.Scan(arrayOfResults...)
		if err != nil {
			return  err
		}
		//转化成结构体
		//跟据反射创建出实例
		pointerOfElement := reflect.New(itemType)
		structValue := reflect.Indirect(pointerOfElement)
		for iIndex, column := range columns {
			field := structValue.FieldByName(column)
			if field != zeroValue {
				if field.CanSet() {
					switch arrayofcolumnType[iIndex] {
					case "NullString":
						lNullString, lok := arrayOfResults[iIndex].(*sql.NullString)
						if lok {
							if !lNullString.Valid {
								field.SetString("")
							} else {
								field.SetString(lNullString.String)
							}
						}
					case "NullInt64","NullInt32":
						lNullInt64, lok := arrayOfResults[iIndex].(*sql.NullInt64)
						if lok {
							if !lNullInt64.Valid {
								field.SetInt(0)
							} else {
								field.SetInt(lNullInt64.Int64)
							}
						}
					case "NullFloat64":
						lNullFloat64, lok := arrayOfResults[iIndex].(*sql.NullFloat64)
						if lok {
							if !lNullFloat64.Valid {
								field.SetFloat(0)
							} else {
								field.SetFloat(lNullFloat64.Float64)
							}
						}
					case "NullBool":
						lNullBool, lok := arrayOfResults[iIndex].(*sql.NullBool)
						if lok {
							if !lNullBool.Valid {
								field.SetBool(false)
							} else {
								field.SetBool(lNullBool.Bool)
							}
						}
					case "NullTime":
						lNullTime, lok := arrayOfResults[iIndex].(*sql.NullTime)
						if lok {
							if !lNullTime.Valid {
								//field.Set
							} else {

							}
						}
					case "byte":
						lbyte, lok := arrayOfResults[iIndex].(*[]byte)
						fmt.Println(lbyte,lok)
					}

				}
			}
		} //添加切片
		recordsValue = reflect.Append(recordsValue, pointerOfElement)
	}
	rv.Elem().Set(recordsValue)
	return  nil
}
