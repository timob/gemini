/*
package gemini

De-normalize data from SQL database table into memory tables (arrays).
Data is organized in Star schema fashion with a fact table and dimension tables
which index it. Meant for creating data driven user displays.

* Each column in DB table results in a dimension table with its unique values.
* Except if column name is in SourceColumnProperties map and is asigned to an 
  another dimension table using PartOfDim.
* The order of the dimension tables can be assigned in SourceColumnProperty
* The fact table only contains dimension table row ids, tying the dimension 
  tables together.
* Uses Sqlite to do table manipulation

example definiton:

d := gemini.Datamart{
    SourceTable: myTableInfo
    SourceColumnProperties: map[string]gemini.SourceColumnProperty{
        "distance": stardisplay.SourceColumnDefinition{
            PartOfDim: "stop_ids",
        },
        "route_short_name" : gemini.SourceColumnProperty{
            SortExpr: "convert(route_short_name, signed)",
        },
        "destination_distance" : gemini.SourceColumnProperty{
            SortDirection: "desc",
        },
    },
}
*/
package gemini

import (
    "fmt"
    "sqlite"
)

const (
    Desc string = "desc"
    Asc string = "asc"
)

type Datamart struct {
    SourceTableData *TableInfo
    SourceColumnProperties map[string]SourceColumnProperty
}

type SourceColumnProperty struct {
    SortExpr string
    SortDirection string    
    PartOfDim string
}

type DimensionDefinition struct {
    IndexColumn string
    UniqueColumn string
    ExtraColumns []string
    SortExpr string
    SortDirection string
}

func (d *Datamart) findDatatype(column string) string {
    for i, v := range d.SourceTableData.ColumnNames {
        if (column == v) {
            return mapDatatypeToSqlite[d.SourceTableData.ColumnTypes[i]]
        }
    }
    return ""
}

// Return map of dimension name to DimDefinition 
func (d *Datamart) SetupDimDefinitions() map[string]*DimensionDefinition {
    dimDefs := make(map[string]*DimensionDefinition)

    // populate dimDefs using column names in SourceTableData.ColumnNames 
    // and any options in SourceColumnProperties
    for _, name := range d.SourceTableData.ColumnNames  {
        prop, ok := d.SourceColumnProperties[name]
        if !ok || prop.PartOfDim == "" {
            dim := new(DimensionDefinition)
            dim.IndexColumn = name + "_id"
            dim.UniqueColumn = name
            if  ok && prop.SortExpr != "" {
                dim.SortExpr = prop.SortExpr
            } else {
                dim.SortExpr = dim.UniqueColumn
            }
            if ok && prop.SortDirection != "" {
                dim.SortDirection = prop.SortDirection
            } else {
                dim.SortDirection = "asc"
            }            
            dimDefs[name + "s"] = dim
        } 
    }

    // add any source columns properties with PartOfDim set to the correct dim
    for name, prop := range d.SourceColumnProperties {
        if prop.PartOfDim != "" {
            dimDefs[prop.PartOfDim].ExtraColumns = append(
                dimDefs[prop.PartOfDim].ExtraColumns,
                name,
            )
        }
    }

    return dimDefs
} 


func (d *Datamart) CreateDimensionTables(dimDefs map[string]*DimensionDefinition,
                                         conn *sqlite.Conn) error {
    var queryStr string
    var err error
    for name, dim := range dimDefs {
        // "integer primary key" creates as auto incrementing column
        queryStr = fmt.Sprintf(
            "create table %s (%s integer primary key, %s %s",
            name,
            dim.IndexColumn,
            dim.UniqueColumn,
            d.findDatatype(dim.UniqueColumn),
        )
        extraStr := ""
        for _, extra := range dim.ExtraColumns {
            queryStr += fmt.Sprintf(", %s %s", extra, d.findDatatype(extra))
            extraStr += "," + extra
        }
        queryStr += "); "

        err = conn.Exec(queryStr)
        if err != nil {
            goto Error
        }

        queryStr = fmt.Sprintf(
            `insert into %s 
             select null, %s %s
             from (select %s, %s sort %s
                   from source
                   where %s is not null
                   group by %s, sort %s) a
             order by sort %s;`,
             name,
             dim.UniqueColumn,
             extraStr,
             dim.UniqueColumn,
             dim.SortExpr,
             extraStr,
             dim.UniqueColumn,
             dim.UniqueColumn,
             extraStr,
             dim.SortDirection,
         )

        err = conn.Exec(queryStr)
        if err != nil {
            goto Error
        }
        
        queryStr = fmt.Sprintf(
            "create index %s_idx on %s (%s);",
            dim.UniqueColumn,
            name,
            dim.UniqueColumn,
        )

        err = conn.Exec(queryStr)
        if err != nil {
            goto Error
        }
    }

    return nil

    Error:
        return fmt.Errorf(
            "CreateDimensionTables() error, sqlite error: %s\nquery:\n%s\n",
            err.Error(),
            queryStr,
        )    
}

func getRowCount(name string, conn *sqlite.Conn) (int, error) {

    stmt, err := conn.Prepare(fmt.Sprintf("select count(1) from %s;", name))
    if err != nil {
        return -1, err
    }
    err = stmt.Exec()
    if err != nil {
        return -1, err
    }
    stmt.Next()    
    var count int 
    err = stmt.Scan(&count)
    if err != nil {
        return -1, err
    }
    return count, nil;
}


// Create fact table by joing source table to dimension tables
// Three cases for source data column:
// 1. Matches dimension column, fact row value is id in matching dimesnion table row
// 2. Is null, and dimension table for column has > 0 rows, fact row value is -1
// 3. Is null, and dimension table for column has 0 rows, fact row value is -1
func (d *Datamart) CreateFactTable(dimDefs map[string]*DimensionDefinition,
                                   conn *sqlite.Conn) error {

    rowCountCache := make(map[string]int)

    // make list of non-zero dim tables
    nzDim := make(map[string]*DimensionDefinition)
    for name, dim := range dimDefs {
        count, err := getRowCount(name, conn)
        if err != nil {
            return err
        }
        rowCountCache[name] = count
        if count > 0 {
            nzDim[name] = dim
        }
    }

    // make fact table
    query := "create table fact as select"
    i := 0
    for name, dim := range dimDefs {        
        if i != 0 {
            query += ","
        }
        if (rowCountCache[name] == 0) {
            query += " -1 " + dim.IndexColumn
        } else {
            query += " case when source." + dim.UniqueColumn + 
                     " is null then -1 else " + dim.IndexColumn + " end " + 
                     dim.IndexColumn
        }
        i++
    }
    
    query += "\nfrom source"
    for name, _ := range nzDim {
        query += ", " + name
    }
    query += "\nwhere "
    i = 0
    for name, dim := range nzDim {
        if i != 0 {
            query += " and "
        }
        query += "(("        
        query += "source." + dim.UniqueColumn + " = " +  
                 name + "." + dim.UniqueColumn
        query += ") or (source." + dim.UniqueColumn + " is null))"
        i++
    }

    // if all dimension tables a empty return emtpy fact table
    if i == 0 {
        query += "1 = 0"
    }

    query += ";"

    err := conn.Exec(query)
    if err != nil {
        return fmt.Errorf(
            "CreateFactTable() error, sqlite error: %s\nquery:\n%s\n",
            err.Error(),
            query,
        )    
    }

    return nil   
}


func (d *Datamart) PerformQueries() (TableSet, error) {
//    os.Remove("/tmp/blah.db")    
//    conn, err := sqlite.Open("/tmp/blah.db")
    conn, err := sqlite.Open(":memory:")
    if err != nil {
        return nil, err
    }

    err = StoreTableToSqlite(conn, "source", d.SourceTableData)
    if err != nil {
        return nil, err
    }

    dimDefs := d.SetupDimDefinitions()
    
    err = d.CreateDimensionTables(dimDefs, conn)
    if err != nil {
        return nil, err
    }

    err = d.CreateFactTable(dimDefs, conn)
    if err != nil {
        return nil, err
    }
    
    var selects []map[string]string
    selects = append(selects, {"name": "fact", "order": "")
    for name, dim := range dimDefs {
        selects = append(
            selects, 
            {"name": name, "order": "order by " + dim.IndexColumn}
        )
    }

    ret := make(TableSet)        
    for _, sel := range selects{
        query := fmt.Sprintf("select * from %s %s;",sel.name, sel.order)
        stmt, err := conn.Prepare(query)
        if err != nil {
            return nil, fmt.Errorf(
                "PerformQueries() error, sqlite error: %s\nquery:\n%s\n",
                err.Error(),
                query,
            )    
        }
        err = stmt.Exec()
        if err != nil {
            return nil, err
        }
        ret[name], err = LoadTableFromSqlite(stmt)
        if err != nil {
            return nil, err
        }
    }

    return ret, nil
}


