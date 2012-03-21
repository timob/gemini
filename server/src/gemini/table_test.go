package gemini

import (
    "fmt"
    "mysql"
    "sqlite"
    "os"
)

func dieOnError(err error) {
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }    
}

func ExampleLoadTableFromMySQL() {
    db, err := mysql.DialTCP("localhost", "tim", "letmein", "tim")
    
    if err != nil {
        os.Exit(1)
    }
    
    err = db.Query("select name, age, length(name) namelen from tabletest;")
    if err != nil {
        os.Exit(1)
    }

    result, err := db.StoreResult()    
    if err != nil {
        os.Exit(1)
    }    

    info, err := LoadTableFromMySQL(result)
    if err != nil {
        fmt.Println(err)        
        os.Exit(1)
    }

    tables := make(TableSet)
    tables["people"] = info
    fmt.Printf("%v", *tables["people"])
    // Output:
    // name
    // string
}

func ExampleLoadTableFromSqlite() {
    conn, err := sqlite.Open(":memory:")    
    err = conn.Exec("create table x (integer x);")
    dieOnError(err)
    
    err = conn.Exec("insert into x values (1);")
    dieOnError(err)
    
    stmt, err := conn.Prepare("select x, 'hi' jacksons from x;")
    dieOnError(err)

    info, err := LoadTableFromSqlite(stmt)

    err = stmt.Finalize()
    dieOnError(err)

    err = conn.Close()
    dieOnError(err)
    
    tables := make(TableSet)
    tables["stuff"] = info
    fmt.Printf("%v\n", *tables["stuff"])
    // Output:
    // blah
}
