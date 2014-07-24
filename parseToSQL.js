var fs = require('fs');
var util = require('util');
var stream = require('stream');
var es = require("event-stream");
var LineByLineReader = require('line-by-line');
var sqlite3 = require('sqlite3').verbose();
var path = require("path");
var _ = require("underscore");

var parseToSQL = function(opts) {
    //instantiate a new parseToSQL object
    this.lineNr = 0;
    this.errLine = 0;
    this.processCount = 0;
    this.processTotal = 0;
    this.files_path = opts.files_path;
    this.file_type = opts.file_type;
    this.db = new sqlite3.Database(opts.db_name);
    this.create_statement = opts.create_statement;
    this.insert_statement = opts.insert_statement;
    this.parseData = opts.parseData;
};

parseToSQL.prototype.processFile = function(file) {
    //process each line in the file and create a new insert statement for each line;
    var that = this;
    var stmt = this.db.prepare(this.insert_statement);
    //process the file as stream of lines
    var lr = new LineByLineReader(file);

    lr.on('error', function (err) {
        // 'err' contains error object
    });

    lr.on('line', function (line) {
        //parse the data for the provded line of text and use it to create a new insert statement
        var data = that.parseData(line);
        if (data){
            stmt.run(data);
            that.lineNr++;
            console.log("insert row: "+ that.lineNr);
        } else {
            that.errLine++;
        }
    });

    lr.on('end', function () {
        //end is called when all lines in the file has been processed;
        console.log("done processing inserts");
        console.log("number of errors: "+ that.errLine);
        stmt.finalize();
        console.log(" done finalizing");
        that.processTotal++;
        if(that.processCount == that.processTotal) {
            that.db.close();
            that.resetCounters();
            console.log("Done closing database.");
        }
    });
};

parseToSQL.prototype.processFiles = function(files){

    var that = this;
    this.processTotal = files.length;
    this.db.serialize(function() {
        //create a new table if exists
        that.db.run(that.create_statement);
        for (var i = 0; i < that.processTotal; i++) {
            //process each file
            that.processFile(files[i]);
        }
    });
};

parseToSQL.prototype.resetCounters = function() {
    this.lineNr = 0;
    this.errLine = 0;
    this.processCount = 0;
    this.processTotal = 0;
};

parseToSQL.prototype.getFileList = function(files){
    //check if the file(s) is/are in the given path are of the type provided by the user ex. "txt", "log",
    var that = this;
    var re = new RegExp("\."+this.file_type+"$");
    //remove any files that are not of the file type from the list such as .DStore
    files = _.filter(files, function(file){
        return re.test(file);
    });
    //create full paths for each file in the list
    return _.map(files, function(file){
        return path.resolve(that.files_path, file);
    });
} ;

parseToSQL.prototype.run = function(){
    var that = this;
    console.log(this.files_path);
    //find all the files in the provided path
    fs.readdir(this.files_path, function (err, files) {
        if (err) {
            throw err;
        }
        console.log(files);
        files = that.getFileList(files);
        console.log('processing files:',files);
        that.processFiles(files);
    });
};

module.exports = parseToSQL;
