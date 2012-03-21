package main

import (
    "fmt"
    "sqlite"
    "mysql"
    "os"
    "gemini"
)

func dieOnError(err error) {
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }    
}

func trysqlite() {
    conn, err := sqlite.Open(":memory:")    
    err = conn.Exec("create table x (a integer);")
    dieOnError(err)
    
    err = conn.Exec("insert into x values (1);")
    dieOnError(err)
    
    stmt, err := conn.Prepare("select a, 'hi' jacksons from x;")
    dieOnError(err)

    info, err := gemini.LoadTableFromSqlite(stmt)
    dieOnError(err)

    err = stmt.Finalize()
    dieOnError(err)

    err = conn.Close()
    dieOnError(err)
    
    tables := make(gemini.TableSet)
    tables["stuff"] = info
    
    fmt.Printf("%v", *tables["stuff"])
}

func trymysql() {
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

    info, err := gemini.LoadTableFromMySQL(result)
    if err != nil {
        fmt.Println(err)        
        os.Exit(1)
    }

    tables := make(gemini.TableSet)
    tables["people"] = info
    fmt.Printf("%v\n", *tables["people"])
}

func main() {
    trysqlite()
    trymysql()
}
