load('gemini.js');
load('data');

function printobject(o) {
    var line = '';
    for(var p in o) {
        line += p + ': ' + o[p] + ' ';
    }
    print(line);
}

function printobjectarray(a) {
    for (var i = 0; i < a.length; i++) {
        printobject(a[i]);
    }
}

function testGeminiresult() {
    var x = new GeminiResult(new Object());
    var y = new GeminiResult({"hello": "world"});
    x.add(y);
    y = new GeminiResult({"goodbye": "world"});
    x.add(y);
    printGeminiResult(x);
}

testGeminiresult();

function testGeminitable() {
    var x = new GeminiTable({
        ColumnNames: ["name", "age"],
        ColumnTypes: ["string", "integer"],
        Data: [["tim", 35], ["scarlet", 27]]
    });
    
    x.addAlias('name', 'person');
    
    var y = x.getRowMap(0);
    printobject(y);
    y = x.getRowMap(1);
    printobject(y);
}

testGeminitable();

function testGeminidb() {
    var x = new GeminiDb(JSON.parse(jsonNextArrivals));
    var y = x.factLookup(0);
    printobject(y);
}

testGeminidb();

function testGeminiQuery() {
    var x = new GeminiDb(JSON.parse(jsonNextArrivals));
    var y = x.newQuery();
    y.addFromTable('route_short_names', 'route_long_names', "date_times");
    y.addClause(function (row) {
        return (row.route_short_name != 3);
    });
    var z = y.slicendice();
    printGeminiResult(z);

    var a = x.newQuery();
    a.addFromTable('route_short_names', 'route_long_names', "date_times");
    a.addClause(function (row) {
        return (row.route_short_name != 44);
    });
    var b = a.simplesort();
    printobjectarray(b);    
}

testGeminiQuery();

function testnull() {
    var x = new GeminiDb({
        "fact" : {
            ColumnNames: ["name_id", "age_id"],
            Data: [[0, 0], [1, -1]]
        },
        "names" : {
            ColumnNames: ["name_id", "name"],
            Data: [[0, "tim"], [1, "scarlet"]]
        },
        "ages" : {
            ColumnNames: ["age_id", "age"],
            Data: [[0, 35]] 
        }
    });
    var y = x.factLookup(1);
    printobject(y);
    var z = x.newQuery().addFromTable('names', 'ages');
    printobjectarray(z.simplesort());
    printGeminiResult(z.slicendice());
    var a = x.newQuery().addFromTable( 'ages', 'names');
    printGeminiResult(a.slicendice());
}

testnull();
