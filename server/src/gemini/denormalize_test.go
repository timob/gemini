package gemini

import (
    "mysql" 
    "testing"
    "fmt"
    "bytes"
    "io/ioutil"
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
    
    var buf bytes.Buffer
    err = tables.JSONWrite(&buf)
    fatalOnError(err, t)
    js, err := ioutil.ReadAll(&buf)    
    fmt.Println(string(js))        
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

    var buf bytes.Buffer
    err = tables.JSONWrite(&buf)
    fatalOnError(err, t)
    js, err := ioutil.ReadAll(&buf)    
    fmt.Println(string(js))
}

