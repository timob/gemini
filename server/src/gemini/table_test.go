package gemini

import (
    "mysql"
    "sqlite"
    "testing"
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

    tables := make(TableSet)
    tables["people"] = info
    
    t.Logf("here %v", *tables["people"])
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
    
    err = stmt.Finalize()
    fatalOnError(err, t)

    err = conn.Close()
    fatalOnError(err, t)
    
    tables := make(TableSet)
    tables["stuff"] = info
    
    t.Logf("%v\n", *tables["stuff"])
}

func TestStoreTableToSqlite(t *testing.T) {
    conn, err := sqlite.Open(":memory:")
    fatalOnError(err, t)
    
    tableInfo := &TableInfo{
        ColumnNames : []string{"name", "age", "height"},
        ColumnTypes : []ColumnDatatype{
            StringDatatype,
            IntegerDatatype,
            FloatDatatype,
        },
        Data : [][]interface{}{{"tim", nil, 1.8}, {"anna", 30, 1.5}},
    }

    err = StoreTableToSqlite(conn, "people", tableInfo)
    fatalOnError(err, t)

    err = conn.Close()
    fatalOnError(err, t)
}

