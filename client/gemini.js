/*
    Gemini, functions to group and sort fact table (array) data using
    dimension tables (array).

    Todo:
    * Look at aditional callback function to do group by functions ie (max/min)
    * Look at adding specified sort order 
    * Look at adding number of rows returned
*/

//
// GeminiTable Class
//
function GeminiTable(tableInfo)
{
    this.data = tableInfo.Data;
    this.columnNames = tableInfo.ColumnNames;
    this.columnTypes = tableInfo.ColumnTypes;
    this.columnAliases = new Object();
}

GeminiTable.prototype.getRowMap = function(rowNum) {
    var retVal = new Object();
    for (var i = 0; i < this.columnNames.length; i++) {
        retVal[this.columnNames[i]] = this.data[rowNum][i];
        if (this.columnAliases[this.columnNames[i]] != undefined) {
            for (var j = 0; j < this.columnAliases[this.columnNames[i]].length; j++) {
                retVal[this.columnAliases[this.columnNames[i]][j]] = this.data[rowNum][i];
            }
        }
    }
    return retVal;
};

GeminiTable.prototype.addAlias = function(columnName, alias) {
    if (this.columnAliases[columnName] == undefined) {
        this.columnAliases[columnName] = new Array();
    }
    this.columnAliases[columnName].push(alias);
};


//
// GeminiDb Class
//
function GeminiDb(source, dataHash)
{
    for (var table in source) {
        this[table] = new GeminiTable(source[table]);
    }

    if (this.fact == undefined) {
        throw new Error("Gemini::constructor can't find fact table");
    }
    
    if (dataHash != undefined) {
        this.dataHash = dataHash;
    }
}

GeminiDb.prototype.joinFactToDim = function(factRow)  {
    var retVal = new Object();
    for (var factColName in factRow) {        
        var tableName = factColName.slice(0, -3) + 's';
        if (factRow[factColName] == -1) {
            retVal[factColName] = -1;
        } else {
            var dimRow = this[tableName].getRowMap(factRow[factColName]);
            for (var dimColName in dimRow) {
                retVal[dimColName] = dimRow[dimColName];
            }
        }
    }
    return retVal;
}

GeminiDb.prototype.factLookup = function(rowNum)  {
    var factRow = this.fact.getRowMap(rowNum);
    return this.joinFactToDim(factRow);
};

GeminiDb.prototype.idForTable = function(tableName) {
    return tableName.slice(0, -1) + "_id";
};

GeminiDb.prototype.addAlias = function(tableName, columnName, alias) {
    this[tableName].addAlias(columnName, alias);
};

GeminiDb.prototype.newQuery  = function() {
    return new GeminiQuery(this);
};

//
// GeminiResult Class
//
function GeminiResult(db, tableName) {
    this.subGroup = this[tableName] = new Array();
    for (var i = 0; i < db[tableName].columnNames.length; i++) {
        this[db[tableName].columnNames[i]] = new Array();    
    }
}

function printGeminiResult(result, depth) {
    if (arguments.length == 1) {
        depth = 0;
    }

    if (result == null) {
        return;
    }
    for (var i = 0; i < result.subGroup.length; i++) {
        var line = '';
        for (var prop in result) {
            if (prop == "subGroup" || result[prop] == result.subGroup) {
                continue;
            }
            for (var j = 0; j < (depth)*4; j++) {
                line += ' ';
            }
            line += prop + ': ' + result[prop][i] + ' ';
        }
        if (line != '') {
            print(line);
        }        
        printGeminiResult(result.subGroup[i], depth + 1);        
    }
};

//
// GeminiQuery Class
//
function GeminiQuery(geminiDb)
{
    this.db = geminiDb;
    this.fromTables = new Array();
}

GeminiQuery.prototype.addFromTable = function () {
    for (var i = 0; i < arguments.length; i++) {
        if (this.db[arguments[i]] === undefined) {
            throw new Error(
                'GeminiQuery::addFromTable Cant find ' + 
                arguments[i] + 
                ' in tables db object.'
            );
        }
        this.fromTables.push(arguments[i]);
    }
    return this;
};

GeminiQuery.prototype.addClause = function (clauseFunc) {
    this.clauseFunc = clauseFunc;
    return this;
};

GeminiQuery.prototype.selectRows = function() {
    // select rows from fact table
    var selectedArray = new Array();
    var unique = new Object();
    for (var i = 0; i < this.db.fact.data.length; i++) {
        var row = this.db.factLookup(i);
        if (this.clauseFunc != undefined) {
            if (this.clauseFunc(row) == false) {
                continue;
            }
        }
        
        var key = new Array();
        for (var j = 0; j < this.fromTables.length; j++) {
            key.push(row[this.db.idForTable(this.fromTables[j])]);
        }
        
        if (unique[key.join('-')] == undefined) {
            unique[key.join('-')] = 1;
            selectedArray.push(key);
        }
    }

    return selectedArray;    
};

GeminiQuery.prototype.calcCacheHash = function() {
    if (this.db.dataHash != undefined) {
        if (this.clauseFunc != undefined) {
            queryHash = hexMD5(
                this.fromTables.join('') + this.clauseFunc.toString()
            );
        } else {
            queryHash = hexMD5(this.fromTables.join(''));
        }
        return queryHash + this.db.dataHash;
    } else {
        return null;
    }
}

GeminiQuery.prototype.slicendice = function() {
    var cacheHash = this.calcCacheHash();
    if (cacheHash != null) {
        var cacheHit = sessionStorage.getItem(cacheHash);
        if (cacheHit != null) {
            console.log('gemini cache hit');
            return JSON.parse(cacheHit);
        }
    }

    var selectedArray = this.selectRows();
    
    // group and sort results into tree structure
    function expand(flatIndex, parentRoot, depth) {
        var subGroups = new Object(); // map unique value at this level to subgroup
        var groupList = new Array(); // ordered list of unique values at this level

        for (var i = 0; i < flatIndex.length; i++) {
            var indexValue = flatIndex[i][this.fromTables.length - 1 - depth];
            if (subGroups[indexValue] === undefined) { // if not seen value before add subgroup and value
                subGroups[indexValue] = new Array(); 
                groupList.push(indexValue);            
            }
            if (depth != 0) {
                subGroups[indexValue].push(flatIndex[i]);
            }
        }

        var sorted = groupList.sort(function(a,b) {
            return a - b;
        });
        
        for (var i = 0; i < sorted.length; i++) {
            var tableName = this.fromTables[this.fromTables.length - 1 - depth];
            var row;
            for (var j = 0; j < this.db[tableName].columnNames.length; j++) {
                if (sorted[i] == -1) {
                    parentRoot[this.db[tableName].columnNames[j]].push(null);
                } else {
                    parentRoot[this.db[tableName].columnNames[j]].push(
                        this.db[tableName].data[sorted[i]][j]
                    );
                }
            }
            
            if (subGroups[sorted[i]].length > 0) {
                // new result for subroup
                var result = new GeminiResult(
                    this.db, 
                    this.fromTables[this.fromTables.length - depth]
                );
                // add subgroup to array in parent result
                parentRoot[tableName].push(result); 
                // process sub group for this value specifing this as the parent
                // in recursive call
                expand.call(this, subGroups[sorted[i]], result, depth - 1);
            } else {
                // no subgroups so add null to array in result
                parentRoot[tableName].push(null);
            }
        }
    }

    var results = new GeminiResult(this.db, this.fromTables[0]);
    expand.call(this, selectedArray, results, this.fromTables.length -1);

    if (cacheHash != null) {
        var item = JSON.stringify(results);
        if (item.length < 1024*1024) {
            console.log('Gemini caching result')
            sessionStorage.setItem(cacheHash, item);
        }
    } 
    
    return results;
};

GeminiQuery.prototype.simplesort = function() {
    var selectedArray = this.selectRows();
    
    var sorted = selectedArray.sort(function(a, b) {
        for (var i = 0; i < a.length; i++) {
            if (a[i] > b[i]) {
                return 1;
            } else if (a[i] < b[i]) {
                return -1;
            } else {
                continue;
            }
        }
    });

    var results = new Array();
    for (var i = 0; i < sorted.length; i++) {
        var factRow = new Object();
        for (var j = 0; j < this.fromTables.length; j++) {
            factRow[this.db.idForTable(this.fromTables[j])] = sorted[i][j];           
        }
        results.push(this.db.joinFactToDim(factRow));
    }
    return results;
}

