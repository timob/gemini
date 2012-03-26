package gemini

import (
    "mysql" 
    "testing"
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
