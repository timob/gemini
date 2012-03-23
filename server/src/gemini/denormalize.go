/*
package gemini

De-normalize data from SQL database table into memory tables (arrays).
Data is organized in Star schema fashion with a fact table and dimension tables
which index it. Meant for creating data driven user displays.

* Each column in DB table results in a dimension table with its unique values.
* Except if column name is in SourceColumnDefinition map and is asigned to an 
  another dimension table using PartOfDim.
* The order of the dimension tables can be assigned in SourceColumnDefinition
* The fact table only contains dimension table row ids, tying the dimension 
  tables together.

example definiton:

sd := stardisplay.StarDisplay{
    SourceTableName: "mytable",
    SourceTable: myTableInfo
    SourceColumnDefinitions: map[string]stardisplay.SourceColumnDefinition{
        "distance": stardisplay.SourceColumnDefinition{
            PartOfDim: "stop_ids",
        },
        "route_short_name" : stardisplay.SourceColumnDefinition{
            SortExpr: "convert(route_short_name, signed)",
        },
        "destination_distance" : stardisplay.SourceColumnDefinition{
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
    queryStr := ""
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

        err := conn.Exec(queryStr)
        if err != nil {
            return fmt.Errorf(
                "CreateDimensionTables() error, sqlite error: %s\nquery:\n%s\n",
                err.Error(),
                queryStr,
            )
        }

        queryStr := fmt.Sprintf(
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
            return fmt.Errorf(
                "CreateDimensionTables() error, sqlite error: %s\nquery:\n%s\n",
                err.Error(),
                queryStr,
            )
        }
    }

/*
        query := `
            set @count = -1;
            create temporary table %s as
            select @count := @count + 1 %s, %s %s
            from (select %s, %s sort %s
                  from %s
                  where %s is not null
                  group by %s, sort %s) a 
            order by sort %s;
        `
        query = fmt.Sprintf(query, s.Dim[i].TableName, s.Dim[i].IndexColumnName,
                            s.Dim[i].UniqueColumn, s.Dim[i].ExtraColumns,
                            s.Dim[i].UniqueColumn, s.Dim[i].SortExpr, 
                            s.Dim[i].ExtraColumns, s.SourceTableName, 
                            s.Dim[i].UniqueColumn, s.Dim[i].UniqueColumn,
                            s.Dim[i].ExtraColumns, s.Dim[i].SortDirection);
        queries += query
    }
    err := db.Query(queries)
    if err != nil {
        return err
    }

    FreeMoreResults(db)

    s.DimData = make(table.TableSet)        
    for i := 0; i < len(s.Dim); i++ {
        query := "select %s, %s %s from %s order by %s;"
        query = fmt.Sprintf(query, s.Dim[i].IndexColumnName,
                            s.Dim[i].UniqueColumn, s.Dim[i].ExtraColumns,
                            s.Dim[i].TableName, s.Dim[i].IndexColumnName)
        err := db.Query(query)
        if err != nil {
            return err
        }

        result, err := db.StoreResult()
        if err != nil {
            return err
        }
                
        s.DimData[s.Dim[i].TableName]= table.LoadTableData(result);

        err = db.FreeResult();
        if err != nil {
            return err
        }
    }
*/
   return nil
}

/*

// Create fact table by joing source table to dimension tables
// Three cases for source data column:
// 1. Matches dimension column, fact row value is id in matching dimesnion table row
// 2. Is null, and dimension table for column has > 0 rows, fact row value is -1
// 3. Is null, and dimension table for column has 0 rows, fact row value is -1
func (s *StarDisplay) CreateFactTable(db *mysql.Client) os.Error {
    // make list of non-zero dim tables
    nzDim := make([]*DimDefinition, 0, len(s.Dim))
    for i := 0; i < len(s.Dim); i++ {
        if len(s.DimData[s.Dim[i].TableName]) > 0 {            
            nzDim = append(nzDim, &s.Dim[i])
        }
    }

    // add indexes to dim tables
    for i := 0; i < len(nzDim); i++ {
        query := fmt.Sprintf("alter table %s add index (%s);",
                             nzDim[i].TableName, nzDim[i].UniqueColumn)
        err := db.Query(query)
        if err != nil {
            return err
        }        
    }

    // make fact table
    query := "select"
    for i := 0; i < len(s.Dim); i++ {
        if i != 0 {
            query += ","
        }
        if (len(s.DimData[s.Dim[i].TableName]) == 0) {
            query += " -1 " + s.Dim[i].IndexColumnName
        } else {
            query += " if(" + s.SourceTableName + "." + s.Dim[i].UniqueColumn + 
                     " is null, -1, " + s.Dim[i].IndexColumnName + ") " + 
                     s.Dim[i].IndexColumnName         
        }
    }
    query += "\nfrom " + s.SourceTableName
    for i := 0; i < len(nzDim); i++ {
        query += ", " + nzDim[i].TableName
    }
    query += "\nwhere "
    for i := 0; i < len(nzDim); i++ {
        if i != 0 {
            query += " and "
        }
        query += "(("        
        query += s.SourceTableName + "." + nzDim[i].UniqueColumn + " = " +  
                 nzDim[i].TableName + "." + nzDim[i].UniqueColumn
        query += ") or (" + s.SourceTableName + "." + nzDim[i].UniqueColumn +
                 " is null))"
    }

    // if all dimension tables a empty return emtpy fact table
    if len(nzDim) == 0 {
        query += "1 = 0"
    }

    query += ";"

    err := db.Query(query)
    if err != nil {
        return err
    }

    result, err := db.StoreResult()
     if err != nil {
        return err
    }

    s.FactData = table.LoadTableData(result);

    err = db.FreeResult();
    if err != nil {
        return err
    }

    return nil   
}

*/

func (d *Datamart) PerformQueries() (TableSet, error) {
    conn, err := sqlite.Open("/tmp/blah.db")
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

    conn.Exec("commit;");
    if err != nil {
        return nil, err
    }

/*

    err := s.CreateDimensionTables(conn)
    if err != nil {
        return nil, err
    }

    err = s.CreateFactTable(db)
    if err != nil {
        return nil, err
    }
    
    ret := make(table.TableSet)
    for k, v := range s.DimData {
        ret[k] = v
    }
    ret[s.FactTableName] = s.FactData
*/
    
    return nil, nil
}

/*

func (s StarDisplay) String() string {
    var str string
    str += "Fact table: " + s.FactTableName + "\n"
    str += s.FactData.String()
    str += "Dim tables:\n"
    str += s.DimData.String()
    return str
}


func FreeMoreResults(db *mysql.Client) {
    for ; db.MoreResults() ; {
        db.NextResult()
    }
}
*/
