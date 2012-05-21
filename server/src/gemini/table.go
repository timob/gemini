package gemini

import (
    "mysql"
    "fmt"
    "sqlite"
    "errors"
    "regexp"
)

type TableData [][]interface{}

type ColumnDatatype string

const (
    IntegerDatatype  = ColumnDatatype("integer")
    StringDatatype  = ColumnDatatype("string")
    FloatDatatype    = ColumnDatatype("float")
)

type TableInfo struct {
    ColumnNames     []string
    ColumnTypes     []ColumnDatatype
    Data            TableData
}

type TableSet map[string]*TableInfo

func LoadTableFromMySQL(result *mysql.Result) (*TableInfo, error) {
    var info TableInfo
    
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
    
    data := make(TableData, 0, 300000)
    for i := 0;;i++ {
        row := result.FetchRow()
        if row == nil {
            break
        }
        data = append(data, row)
    }
    
    info.Data = data
    return &info, nil
}


func LoadTableFromSqlite(s *sqlite.Stmt) (*TableInfo, error) {
    var info TableInfo
    var cols, colptrs []interface{}
    
    data := make(TableData, 0, 300000)
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
        }

        // assign column values to addresses pointed to by array elements
        err := s.Scan(colptrs...)
        if err != nil {
            return nil, err
        }
        
        // create new array and copy column values into elements of new array
        cols = make([]interface{}, s.ColumnCount())
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
        data = append(data, cols)
    }
    
    // if no rows just create empty arrays for column names, types
    if i == 0 {
            info.ColumnNames = make([]string, 0)
            info.ColumnTypes = make([]ColumnDatatype, 0)
    }
    
    info.Data = data
    s.Finalize()
    return &info, nil
}

var mapDatatypeToSqlite map[ColumnDatatype]string = map[ColumnDatatype]string{
    IntegerDatatype : "numeric",
    StringDatatype : "text",
    FloatDatatype : "real",
}

func StoreTableToSqlite(conn *sqlite.Conn , name string, tinfo *TableInfo) error {
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
    
    for i := 0; i < len(tinfo.Data); i++ {
        queryStr = fmt.Sprintf("insert into %s values (", name)
        for j := 0; j < len(tinfo.Data[i]); j++ {
            if j != 0 {
                queryStr += ","
            }
            value := tinfo.Data[i][j]
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
