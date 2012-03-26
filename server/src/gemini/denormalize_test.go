package gemini

import (
    "mysql" 
    "testing"
    "encoding/json"
    "fmt"
)

var gt *testing.T

func TestDatamart(t *testing.T) {
    gt = t
    db, err := mysql.DialTCP("localhost", "tim", "letmein", "tim")
    fatalOnError(err, t)
        
    err = db.Query("select name, age, length(name) namelen from tabletest;")
    fatalOnError(err, t)

    result, err := db.StoreResult()
    fatalOnError(err, t)

    info, err := LoadTableFromMySQL(result)
    fatalOnError(err, t)
    
    dmart := Datamart{
        SourceTableData: info,
    }
       
    tables, err := dmart.PerformQueries()
    fatalOnError(err, t)
    
    for name, table := range tables {
        t.Logf("name: %s\n%v\n", name, table)
    }
}

func TestDatamart2(t *testing.T) {
    gt = t
    db, err := mysql.DialTCP("localhost", "tim", "letmein", "wellington")
    fatalOnError(err, t)
        
    err = db.Query("call get_next_arrivals('2012-02-04 00:40:00', 10120);")
    fatalOnError(err, t)

    err = db.Query("select * from time_trip;")
    fatalOnError(err, t)

    result, err := db.StoreResult()
    fatalOnError(err, t)

    info, err := LoadTableFromMySQL(result)
    fatalOnError(err, t)
    
    dmart := Datamart{
        SourceTableData: info,
    }
       
    tables, err := dmart.PerformQueries()
    fatalOnError(err, t)

    jsonData, err := json.Marshal(tables)
    fatalOnError(err, t)

    fmt.Printf("%s\n", jsonData)    
}

