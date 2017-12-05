var request = require("request-promise");
var sql = require("mssql");
var _ = require('lodash');

var username = 'robby.barnes@springmobile.com/token'
var password = 'xQAFlodg2Snmm6Eb0QU4BZslvg73cnX1DH0TvKeB'
var auth = "Basic " + new Buffer(username + ":" + password).toString("base64");
var objects = ['Requests']

var sqlConfig = {
    user: 'techbrands_importer',
    password: 'Summet1',
    server: '52.6.249.188',
    database: 'TARDIS'
}

var options = {
    uri: "https://tech-brands.zendesk.com/api/v2/requests.json",
    port: 443,
    method: "GET",
    headers: {
        authorization: auth
    }
}

var json;
var nextPage, currentPageNumber;

let parents = [],
    via = [],
    via_source_from = [],
    via_source_to = [],
    custom_fields = [],
    fields = [];

objects.forEach(function(element) {

    main(element, function() {
        console.log('All done!');
    });
    
}, this);

function main(objectName, callback) {
    
    request(options, (error, response, body) => {

        if(!error && response.statusCode == 200) {

            json = JSON.parse(body).requests;

            splitData();

            console.log(parents);

            saveDataToDb(parents, "staging.Zendesk_" + objectName);
            // console.log("parent: ", parents);
            // console.log("via: ", via);
            // console.log("via_source_from: ", via_source_from);
            // console.log("via_source_to: ", via_source_to);
            // console.log("custom_fields: ", custom_fields);
            // console.log("fields: ", fields);
            
            if(nextPage){
                currentPageNumber = Number(nextPage.substring(nextPage.indexOf('=')+1));
            }
            
            nextPage = JSON.parse(body).next_page;
            
            if(nextPage) {
                currentPageNumber = Number(nextPage.substring(nextPage.indexOf('=')+1)-1);
                options.uri = nextPage;
                main(objectName, callback);
                callback(1);
            } else {
                callback(1);
            }

            console.log('Finished processing page ' + currentPageNumber + ' of ' + objectName);

        } else {

            console.log(error);
            throw error;

        }
    });
}

function splitData(){
    
    json.forEach( record => {
    
        let pid = record.id;

        parents = [ ...parents, record ];

        via = [ ...via, Object.assign({}, { parent_id: pid, channel: record.via.channel } ) ];

        via_source_from = [ ...via_source_from, Object.assign({}, { parent_id: pid }, record.via.source.from ) ]

        via_source_to = [ ...via_source_to, Object.assign({}, { parent_id: pid }, record.via.source.to ) ]

        custom_fields = [ ...custom_fields, ...record.custom_fields.map( f => { return Object.assign({}, f, { parent_id: pid }) } ) ]

        fields = [ ...fields, ...record.fields.map( f => { return Object.assign({}, f, { parent_id: pid }) } ) ]
    
    });
    
    Object.keys(parents).forEach(key => {
        delete parents[key].via;
        delete parents[key].custom_fields;
        delete parents[key].fields;
    });
}








function transform(records, allowed = {}, add = {}) {

    const final = {};
    Object.keys(allowed).forEach( key => final[key] = [] );

    records.forEach( record => {

        const add_prop = {};
        Object.keys(add).forEach( key => add_prop[key] = _.property(add[key])(record) )

        Object.keys(final).forEach( key => {

            current_prop = _.property(allowed[key])(record);

            if (current_prop instanceof Array) {

                final[key] = [
                    ...final[key],
                    ...current_prop.map( o => Object.assign({}, add_prop, o) )
                ];
            }
            else if (typeof current_prop === "string"){

                const obj_key = allowed[key].split('.').pop();
                final[key] = [ ...final[key], Object.assign({}, add_prop, { [obj_key]: current_prop}) ];
            }
            else {

                final[key] = [ ...final[key], Object.assign({}, add_prop, current_prop) ];
            }

        });

    });

    return final;
}

function getChildren(requests){
    var obj = {};
    var objKey, objValue;

    function testInner(object, objKey, objValue){
        _.forIn(object, (value, key) => {
            var oKey = objKey + '_' + key;
            var oValue = objValue + '.' + key;
            if(value instanceof Object && value != null) {
                if(!(oKey in obj)){
                    obj[oKey] = oValue;
                }
            } else if (value != null){
                if(!(oKey in obj)){
                    obj[objKey] = oValue;
                }
            }
        })
        return 
    }
    
    _.forIn(requests, (value,key) => {
       _.forIn(value, (value, key) =>{
            // var one, two;

            if(value instanceof Array && value != []){
                if(!(key in obj)){
                    obj[key] = key;
                }
            } else if(value instanceof Object && value != null) {
                objKey = key;
                objValue = key;
                _.forIn(value, (value, key) => {
                    if(value instanceof Object && value != null) {
                        objKey += '_' + key;
                        objValue += '.' + key;
                        testInner(value, objKey, objValue);
                        // _.forIn(value, (value, key) => {
                        //     if(typeof(value) == 'object' && value != null) {
                        //         if(!(one + '_' + two + '_' + key in obj)){
                        //             obj[one + '_' + two + '_' + key] = one + '.' + two + '.' + key;
                        //         }
                        //     }
                        // })
                    } else if (value != null){
                        if(!(objKey in obj)){
                            obj[objKey] = objValue;
                        }
                    } else {

                    }
                })
            }
        });
    })

    return obj;
}









var name;

function findIt(element){
    return element.name === name;
}

function getElementLengths(json){

    // Takes a json object as argument.
    // Returns an array with the max length of each elements values in the object.
        // [ { name: 'Element Name', len: 10 } ]
    // Gets the max length of all the JSON element values and stores them to an array of objects.

    var arr = [];

    Object.keys(json).forEach(element => {
        Object.keys(json[element]).forEach(innerElement => {

            name = innerElement
            var found = arr.find(findIt)
            var len = JSON.stringify(json[element][innerElement]).length;
            
            if(found){
                if(found.len < len){
                    found.len = len;
                }
            } else {
                var missingObj = {name: name, len: len};
                arr.push(missingObj);
            }

        }, this);
    }, this);

    return arr;

}

function prepareData(json, tableName){

    // Takes a JSON string and a name for the table.
    // Returns a sql.Table object populated by the supplied JSON data named after the tableName parameter.
    // Prepares the JSON data to be stored to the SQL Server.

    var elementLen = getElementLengths(json);
    const table = new sql.Table(tableName);
    table.create = true;
    
    // Add a column to the table for each of the elements in JSON 
    Object.keys(elementLen).forEach(element => {

        name = elementLen[element].name;
        found = elementLen.find(findIt);
        var len = found.len > 5000 ? 8000 :
            found.len > 1000 ? 5000 :
            found.len > 255 ? 1000 : 255;
        table.columns.add(name, sql.varChar(len), {nullable: true})

    });

    // Add each of the JSON objects to the table as individual rows.
    Object.keys(json).forEach(element => {

        var row = [];

        Object.keys(json[element]).forEach(innerElement => {
            row.push(json[element][innerElement]);
        }, this);

        table.rows.add(...row);
        row = [];

    });

    return table;

}

function saveDataToDb(json, tableName) {

    // Takes a JSON string and a name for the table.
    // Saves the data prepared JSON data to the SQL Server.

    var table = prepareData(json, tableName);

    // Save the data to the SQL Server
    var connection = sql.connect(sqlConfig, (pool) => {

        var request = new sql.Request(pool);
        var result = request.bulk(table, (err) => {

            if(err){
                console.log("fail. " + err);
            }

            sql.close();
            
        })

        request.on('error', err => {
            console.log('request error: ' + err);
        })

    })

    connection.on('error', err => {
        console.log('connection error: ' + err);
    })
}