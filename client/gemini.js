/*
    Gemini, functions to group and sort fact table (array) data using
    dimension tables (array).

    Todo:
    * Look at changing filter function arguments pass as an object
    * Look at aditional callback function to do group by functions ie (max/min)
    * Look at adding specified sort order 
    * Look at adding number of rows returned

    done but needs work:
    * Look at being able to alias column names (done but inefficent)
*/

function GeminiDb(source)
{
    this.tables = source;
    for (var table in tables) {
        this[table] = tables[table];
    }

    if (this.fact == undefined) {
        throw new Error("Gemini::constructor can't find fact table");
    }
}

GeimiDb.prototype.newQuery()  = function() {
    return new GeminiQuery(this);
};

function GeminiResult() = function() {
    this.length = 0;    
}

function GeminiQuery(geminiDb)
{
    this.geminiDb = geminiDb;
    this.fromTables = new Array();
}

GeminiQuery.prototype.addFromTable() = function () {
    for (var i = 0; i < arguments.length; i++) {
        this.fromTables.push(arguments[i]);
    }
    return this;
};

GeminiQuery.prototype.addFilterFunc() = function (filter {
    this.filter = filter;
    return this;
};

GeminiQuery.prototype.slicendice = function()
{
    
};

StarSchema.prototype.slicendice = function(queryTablesSpec, filterFunction)
{
    var queryTables = queryTablesSpec.replace(/ /g, '').split(',');
    var groupColumns = new Array();    
    for (var i = 0; i < queryTables.length; i++) {
        if (this.dimTableIdColumns[queryTables[i]] === undefined) {
            throw new Error('StarSchema::slicendice Cant find ' + queryTables[i] + 
                            ' in tables array.');
        }
        groupColumns[i] = this.dimTableIdColumns[queryTables[i]];
    } 

    var groups = new Object();
    var selectedArray = new Array();
    for (var i = 0; i < this.factTable.length; i++) {
        if (filterFunction !== null) {
            filterFunctionArguments = new Array();
            for (var j = 0; j < groupColumns.length; j++) {
                filterFunctionArguments.push(this.factTable[i][groupColumns[j]]);
            }
            if (filterFunction.apply(this, filterFunctionArguments)) {
                continue;
            }
        }
 
        // check if we have seen this permutation before if so dont add
        // to avoid duplicates
        for (var j = 1, exists = groups[this.factTable[i][groupColumns[0]]];
             j < groupColumns.length, exists !== undefined; j++) 
        {
            var exists = exists[this.factTable[i][groupColumns[j]]];
        }

        if (exists == undefined) {
            var ids = new Array();
            for (var j = 0; j < groupColumns.length; j++) {
                ids.push(this.factTable[i][groupColumns[j]]);
            }
            selectedArray.push(ids);
        }        
    } /* i loop */
    
    function expand(flatIndex, parentRoot, depth) { 
        var subGroups = new Object(); // map unique value at this level to subgroup
        var groupList = new Array(); // ordered list of unique values at this level
        
        for (var i = 0; i < flatIndex.length; i++) {
            var indexValue = flatIndex[i][queryTables.length - 1 - depth];
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
            var entry = new Object(); // object that is passed back to caller
            entry.id = sorted[i];
            entry.value = this.dimTables[queryTables[queryTables.length - 1 - depth]][sorted[i]];
            entry.subGroup = new Array();
            parentRoot.push(entry);
            if (subGroups[sorted[i]].length > 0) {
                // process sub group for this value specifing this as the parent
                // in recursive call
                expand.call(this, subGroups[sorted[i]], entry.subGroup, depth - 1);
            }
        }
    };

    var results = new Array();
    expand.call(this, selectedArray, results, queryTables.length -1);
    
    return results;
};

StarSchema.prototype.add_alias = function(table, columnName, alias) {
    for (var i = 0; i < this[table].length; i++) {
        this[table][i][alias] = this[table][i][columnName];
    }
};


