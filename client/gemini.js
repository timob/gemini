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
    for (var i = 0; i < this.columnNames; i++) {
        retVal[this.columnNames[i]] = this.data[rowNum][i];
        for (var j = 0; j < this.columnAliases[this.columnNames[i]].length; j++) {
            retVal[this.columnAliases[this.columnNames[i]][j]] = this.data[rowNum][i];
        }
    }
    return retVal;
};

GeminiTable.prototype.addAdlias = function(columnName, alias) {
    if (this.columnAliases[columnName] == undefined) {
        this.columnAliases[columnName] = new Array();
    }
    this.columnAliases[columnName].push(alias);
};


//
// GeminiDb Class
//
function GeminiDb(source)
{
    for (var table in source) {
        this[table] = new GeminiTable(source[table]);
    }

    if (this.fact == undefined) {
        throw new Error("Gemini::constructor can't find fact table");
    }
}

GeminiDb.prototype.factLookup = function(row)  {
    var retVal = new Object();
    var factRow = this.fact.getRowMap(row);
    for (var factColName in factRow) {
        var tableName = factColName.slice(0, -3) + s;
        var dimRow = this[tableName].getRowMap(factRow[factColName]);
        for (var dimColName in dimRow) {
            retVal[dimColName] = dimRow[dimColName];
        }
    }
    return retVal;
};

GeminiDb.prototype.idForTable = function(tableName) {
    return tableName.slice(0, -1) + "_id";
};

GeminiDb.prototype.addAlias = function(tableName, columnName, alias) {
    this[tableName].addAlias(columnName, alias);
};

GeimiDb.prototype.newQuery()  = function() {
    return new GeminiQuery(this);
};


//
// GeminiResult Class
//
function GeminiResult(object) {
    this.length = 0;   
    for (var prop in object) {
        if (prop == "length" || prop == "add") {
            throw new Error('GeminiResult:: invalid column name');
        }
        this[prop] = object[prop];
    }    
}

GeminiResult.prototype.add = function(newRes) {
    this[this.length] = newRes;
    this.length++;
};


//
// GeminiQuery Class
//
function GeminiQuery(geminiDb)
{
    this.db = geminiDb;
    this.fromTables = new Array();
}

GeminiQuery.prototype.addFromTable() = function () {
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

GeminiQuery.prototype.addFilterFunc() = function (filter) {
    this.filter = filter;
    return this;
};

GeminiQuery.prototype.slicendice = function() {
    // select rows from fact table
    var selectedArray = new Array();
    var unique = new Object();
    for (var i = 0; i < this.db.fact.length; i++) {
        var row = this.db.factLookup(i);
        if (this.filter != undefined) {
            if (this.filter(row)) {
                continue;
            }
        }
        
        var key = new Array();
        for (var j = 0; j < this.fromTables.length; j++) {
            key.push(row[this.db.idForTable[this.fromTables[j]]);
        }
        
        if (unique[key.join('-')] == undefined) {
            unique[key.join('-')] = 1;
            selectedArray.push(key);
        }
    }

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
            return a > b;
        });
        
        for (var i = 0; i < sorted.length; i++) {
            var row = this.db[this.fromTables[this.fromTables.length - 1 - depth]].getRowMap(sorted[i]);
            var result = new GeminiResult(row); // object that is passed back to caller
            parentRoot.add(result);
            if (subGroups[sorted[i]].length > 0) {
                // process sub group for this value specifing this as the parent
                // in recursive call
                expand.call(this, subGroups[sorted[i]], result, depth - 1);
            }
        }
    }

    var results = new GeminiResult();
    expand.call(this, selectedArray, results, this.fromTables.length -1);
    
    return results;
};
