package gemini

import (
    "mysql"
    "fmt"
    "sqlite"
    "errors"
    "regexp"
    "encoding/binary"
    "encoding/json"
    "io"
)

var tableSpace [50*1024*1024]byte
var allocedSpace int = 0

type TableData []byte

type cursor struct {
    data *TableData
    offset int
}

type ColumnDatatype string

const (
    IntegerDatatype  = ColumnDatatype("integer")
    StringDatatype  = ColumnDatatype("string")
    FloatDatatype    = ColumnDatatype("float")
)


type Table struct {
    ColumnNames     []string
    ColumnTypes     []ColumnDatatype
    Data            TableData
    RowOffsets      []int    
}

type TableSet map[string]*Table


func ClearTableSpace() {
    allocedSpace = 0
}

func (t *TableData) Write(p []byte) (int, error) {
    if len(p) + allocedSpace > len(tableSpace) {
        return 0, errors.New("gemini table ran out of table space")
    }
    n := copy(tableSpace[allocedSpace:], p)
    allocedSpace = allocedSpace + n
    *t = tableSpace[allocedSpace - n - len(*t):allocedSpace]
    return n, nil
}

func (c cursor) Read(p []byte) (int, error) {
    return copy(p, (*c.data)[c.offset:]), nil
}

func (t *Table) initData() {
    t.Data = tableSpace[allocedSpace:allocedSpace]
    t.RowOffsets = make([]int, 0)
}

func (t *Table) rowCount() int {
    return len(t.RowOffsets)
}

func (t *Table) writeRow(rowValues []interface{}) error {
    t.RowOffsets = append(t.RowOffsets, len(t.Data))

	for i, v := range t.ColumnTypes {
        if rowValues[i] == nil {
            err := binary.Write(&t.Data, binary.LittleEndian, int16(-1))
            if err != nil {
                return err
            }
            continue
        }
         
        var rep string
		switch v {
		case IntegerDatatype:		    
            rep = fmt.Sprintf("%d", rowValues[i])
		case FloatDatatype:		    
            rep = fmt.Sprintf("%f", rowValues[i])
		case StringDatatype:
            rep = rowValues[i].(string)
	    }	    
	    err := binary.Write(&t.Data, binary.LittleEndian, int16(len(rep)))       
        if err != nil {
            return err
        }
	    (&t.Data).Write([]byte(rep))
	}
	return nil
}

func (t *Table) readRow(rowNum int, rowValues []*interface{}) error {
    offset := t.RowOffsets[rowNum]
	for i, v := range t.ColumnTypes {
	    var size int16
	    var rep string
	    binary.Read(cursor{&t.Data, offset}, binary.LittleEndian, &size)
	    offset = offset + 2
	    if size == -1 {
            *(rowValues[i]) = nil
            continue
	    } else {
	        rep = string(t.Data[offset:offset+int(size)])
	        offset = offset + int(size)
	    } 
		switch v {
		case IntegerDatatype:
		    var value int64
		    _, err := fmt.Sscan(rep, &value)
            if err != nil {
                return err
            }
		    *(rowValues[i]) = value
		case FloatDatatype:
		    var value float64
		    _, err := fmt.Sscan(rep, &value)
            if err != nil {
                return err
            }
		    *(rowValues[i]) = value
		case StringDatatype:
		    *(rowValues[i]) = rep
	    }
	}
	return nil
}


func LoadTableFromMySQL(result *mysql.Result) (*Table, error) {
    var info Table
    
    fields := result.FetchFields()
    info.ColumnNames = make([]string, len(fields))
    info.ColumnTypes = make([]ColumnDatatype, len(fields))
    for i := 0; i < len(fields); i++ {
        info.ColumnNames[i] = fields[i].Name
        switch fields[i].Type {
            case mysql.FIELD_TYPE_VAR_STRING,
	             mysql.FIELD_TYPE_DECIMAL,
	             mysql.FIELD_TYPE_NEWDECIMAL,
                 mysql.FIELD_TYPE_VARCHAR:
                info.ColumnTypes[i] = StringDatatype
            case mysql.FIELD_TYPE_TINY,
             	 mysql.FIELD_TYPE_SHORT,
	             mysql.FIELD_TYPE_LONG,
	             mysql.FIELD_TYPE_LONGLONG,
	             mysql.FIELD_TYPE_INT24:
	            info.ColumnTypes[i] = IntegerDatatype
            case mysql.FIELD_TYPE_FLOAT,
                 mysql.FIELD_TYPE_DOUBLE:
	            info.ColumnTypes[i] = FloatDatatype
            default:
                return nil, errors.New(
                    fmt.Sprintf(
                        "LoadTableFromMySQL unkown type %v\n",
                        fields[i].Type,
                    ),
                )
        }        
    }
    
    info.initData()
    
    for i := 0;;i++ {
        row := result.FetchRow()
        if row == nil {
            break
        }
        err := info.writeRow(row)
        if err != nil {
            return nil, err
        }
    }
    
    return &info, nil
}


func LoadTableFromSqlite(s *sqlite.Stmt) (*Table, error) {
    var info Table
    var cols, colptrs []interface{}
    
    info.initData()
    var i int
    for i = 0;; i++ {
        if s.Next() == false {
            break
        }

        // get table columns info from first row of results
        if i == 0 {
            info.ColumnNames = make([]string, s.ColumnCount())
            info.ColumnTypes = make([]ColumnDatatype, s.ColumnCount())

            // create array of pointers to allocated space for column values
            colptrs = make([]interface{}, s.ColumnCount())            
            for j := 0; j < s.ColumnCount(); j++ {             
                info.ColumnNames[j] = s.ColumnName(j) 
                                
                switch s.ColumnType(j) {
                    case sqlite.IntegerDatatype:
                        info.ColumnTypes[j] = IntegerDatatype 
                        colptrs[j] = new(int)
                    case sqlite.FloatDatatype:
                        info.ColumnTypes[j] = FloatDatatype 
                        colptrs[j] = new(float64)
                    case sqlite.TextDatatype:
                        info.ColumnTypes[j] = StringDatatype
                        colptrs[j] = new(string)
                    default:
                        return nil, errors.New(
                            fmt.Sprintf(
                                "LoadTableFromSqlite unkown type %v\n",
                                s.ColumnType(i),
                            ),
                        )
                }
            }
            
            cols = make([]interface{}, s.ColumnCount())
        }

        // assign column values to addresses pointed to by array elements
        err := s.Scan(colptrs...)
        if err != nil {
            return nil, err
        }
        
        // create new array and copy column values into elements of new array
        for j := 0; j < s.ColumnCount(); j++ {
            switch s.ColumnType(j) {
                case sqlite.IntegerDatatype:
                    cols[j] = *colptrs[j].(*int)
                case sqlite.FloatDatatype:
                    cols[j] = *colptrs[j].(*float64)
                case sqlite.TextDatatype:
                    cols[j] = *colptrs[j].(*string)
                default:
                    return nil, errors.New(
                        fmt.Sprintf(
                            "LoadTableFromSqlite unkown type %v\n",
                            s.ColumnType(i),
                        ),
                    )
            }
        }

        // add columns values as row to table
        err = info.writeRow(cols)
        if err != nil {
            return nil, err
        }
    }
    
    // if no rows just create empty arrays for column names, types
    if i == 0 {
            info.ColumnNames = make([]string, 0)
            info.ColumnTypes = make([]ColumnDatatype, 0)
    }
    
    s.Finalize()
    return &info, nil
}

var mapDatatypeToSqlite map[ColumnDatatype]string = map[ColumnDatatype]string{
    IntegerDatatype : "numeric",
    StringDatatype : "text",
    FloatDatatype : "real",
}

func StoreTableToSqlite(conn *sqlite.Conn , name string, tinfo *Table) error {
    queryStr := fmt.Sprintf("create table %s (", name)
    for i := 0; i < len(tinfo.ColumnNames); i++ {
        if i != 0 {
            queryStr += ","
        }
        queryStr += 
            tinfo.ColumnNames[i] + 
            " "  +
            mapDatatypeToSqlite[tinfo.ColumnTypes[i]]
    }
    queryStr += ");"
    
    err := conn.Exec(queryStr)
    if err != nil {
        return fmt.Errorf("StoreTableToSqlite(): %s , %s,", err.Error(), queryStr)
    }
    
    row := make([]*interface{}, len(tinfo.ColumnTypes))
    for i := 0; i < len(row); i++ {
        row[i] = new(interface{})
    }
    for i := 0; i < tinfo.rowCount(); i++ {
        queryStr = fmt.Sprintf("insert into %s values (", name)
        err := tinfo.readRow(i, row)
        if err != nil {
            return err
        }
        for j := 0; j < len(row); j++ {
            if j != 0 {
                queryStr += ","
            }
            value := *(row[j])
            if value == nil {
                queryStr += "null"
                continue
            }
            switch tinfo.ColumnTypes[j] {
                case IntegerDatatype:
                    queryStr += fmt.Sprintf("%d", value)
                case StringDatatype:
                    re, err := regexp.Compile("'")
                    if err != nil {
                        return err
                    }
                    escaped := re.ReplaceAllString(value.(string), "''")
                    queryStr += fmt.Sprintf("'%s'", escaped)
                case FloatDatatype:
                    queryStr += fmt.Sprintf("%f", value)
            }
        }
        queryStr += ");"
        err = conn.Exec(queryStr)
        if err != nil {
            return fmt.Errorf("StoreTableToSqlite(): %s , %s,", err.Error(), queryStr)
        }
    }
    
    return nil
}

func (t *Table) JSONWrite(w io.Writer) error {
    w.Write([]byte("{\"ColumnNames\":"))
    js,err := json.Marshal(t.ColumnNames)
    if err != nil {
        return err
    }
    w.Write(js)
    w.Write([]byte(", \"ColumnTypes\":"))
    js,err = json.Marshal(t.ColumnTypes)
    if err != nil {
        return err
    }                    
    w.Write(js)
    w.Write([]byte(", \"Data\":["))
    row := make([]*interface{}, len(t.ColumnTypes))
    for i := 0; i < len(row); i++ {
        row[i] = new(interface{})
    }                        
    for i := 0; i < t.rowCount(); i++ {
        if i != 0 {
            w.Write([]byte(","))
        }
        t.readRow(i, row)
        js,err = json.Marshal(row)
        if err != nil {
            return err
        }        
        w.Write(js) 
    }
    w.Write([]byte("]}"))
    return nil
}
        
func (t TableSet) JSONWrite(w io.Writer) error {
    w.Write([]byte("{"))
    i := 0
    for k, v := range t {
        if i != 0 {
            w.Write([]byte(","))
        }
        w.Write([]byte("\""))
        w.Write([]byte(k))
        w.Write([]byte("\":"))
        err := v.JSONWrite(w)
        if err != nil {
            return err
        }
        i++
    }
    w.Write([]byte("}"))
    return nil
}

