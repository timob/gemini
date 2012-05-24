package gemini

import (
    "mysql"
    "sqlite"
    "testing"
    "bytes"
    "io/ioutil"
)


func fatalOnError(err error, t* testing.T) {
    if err != nil {
        t.Fatal(err.Error())
    }    
}

func TestLoadTableFromMySQL(t *testing.T) {
    db, err := mysql.DialTCP("localhost", "tim", "letmein", "tim")
    fatalOnError(err, t)
    
    err = db.Query("select name, age, length(name) namelen from tabletest;") 
    fatalOnError(err, t)

    result, err := db.StoreResult()    
    fatalOnError(err, t)

    info, err := LoadTableFromMySQL(result)
    fatalOnError(err, t)

    var buf bytes.Buffer
    err = info.JSONWrite(&buf)
    fatalOnError(err, t)
    js, err := ioutil.ReadAll(&buf)    
    t.Log(string(js))        
}

func TestLoadTableFromSqlite(t *testing.T) {
    conn, err := sqlite.Open(":memory:")    
    fatalOnError(err, t)
    
    err = conn.Exec("create table x (x integer);")
    fatalOnError(err, t)
    
    err = conn.Exec("insert into x values (1);")
    fatalOnError(err, t)

    stmt, err := conn.Prepare("select x, 'hi' jacksons from x;")
    fatalOnError(err, t)

    info, err := LoadTableFromSqlite(stmt)
    fatalOnError(err, t)

    var buf bytes.Buffer
    err = info.JSONWrite(&buf)
    fatalOnError(err, t)
    js, err := ioutil.ReadAll(&buf)    
    t.Log(string(js))        
}

func TestStoreTableToSqlite(t *testing.T) {
    conn, err := sqlite.Open(":memory:")
    fatalOnError(err, t)
    
    tableInfo := &Table{
        ColumnNames : []string{"name", "age", "height"},
        ColumnTypes : []ColumnDatatype{
            StringDatatype,
            IntegerDatatype,
            FloatDatatype,
        },
    }

    tableInfo.initData()
    tableInfo.writeRow([]interface{}{"tim", 5, 1.1})
    tableInfo.writeRow([]interface{}{"lao", 4, 1.5})
    err = StoreTableToSqlite(conn, "people", tableInfo)
    fatalOnError(err, t)

    err = conn.Close()
    fatalOnError(err, t)
}

