var path = require("path")
var parseToSQL = require("./parseToSQL");

var formatDataForLog = function(matchArray){
    return {
        $i_box: matchArray[1],
        $i_ip: matchArray[2],
        $timestamp: formatDate(matchArray[3]),
        $short_url: matchArray[4],
        $status: matchArray[5],
        $user_agent: matchArray[6],
        $long_url: matchArray[7],
        $i_id: getInstanceId(matchArray[4]),
        $host_url: getHostUrl(matchArray[4]),
        $file_type: getFileType(matchArray[4])
    }
};

var formatDataForImpressions = function(obj){
    return {
        $detail: obj["detail"],
        $userAgent: obj["userAgent"],
        $browserName: obj["browserName"],
        $timestamp: obj["timestamp"],
        $city: obj["city"],
        $instanceUUID: obj["instanceUUID"],
        $platformName: obj["platformName"],
        $domain: obj["domain"],
        $clientTimestamp: obj["clientTimestamp"],
        $registrationUUID: obj["registrationUUID"],
        $osName: obj["osName"]
    };
};

var formatDate = function(dateString){
    var date = new Date(dateString.replace(/\//g," ").replace(":"," "));
//    console.log(dateString);
    return date.toISOString();
};

var getFileType = function(url){
    var re = /\/syndication\/([^\?\/]+)/
    var matchArray = url && url.match(re);
    return matchArray ? matchArray[1] : "-";
};

var getInstanceId = function(url){
    var re = /i=([^\&]+)/

    var matchArray = url && url.match(re);
    return matchArray ? matchArray[1] : null;
};
var getHostUrl = function(url){
    var re = /&d=([^\&]+)/
    var matchArray = url && url.match(re);
    try {
        return matchArray ? decodeURIComponent(matchArray[1]) : null;    
    } catch (err) {
        console.log(err);
        return matchArray[1];
    }    
};

//opt parameters are : files_path, file_type, db_name, create_statement, insert_statement
var logToSQL = new parseToSQL({
    files_path: path.resolve(__dirname, 'logs'),
    file_type: "log",
    db_name: "AccessLog20140717",
    create_statement: "CREATE TABLE IF NOT EXISTS logs (i_id TEXT, timestamp TEXT, file_type TEXT,status TEXT, i_box TEXT, i_ip TEXT, user_agent TEXT, host_url TEXT, short_url TEXT, long_url TEXT)",
    insert_statement: "INSERT INTO logs VALUES ($i_id, $timestamp, $file_type, $status, $i_box, $i_ip, $user_agent, $host_url, $short_url, $long_url)",
    parseData: function(line) {
        var re = /^.* (.+) (.+) - - (?:\[(.*)\]) "([^\"]*)" (\d{3}) (?:[^ ]+) "(?:[^\"]*)" "([^\"]*)" (?:[^ ]+) (?:[^ ]+) (?:[^ ]+) (?:[^ ]+) (.*)$/;
        //instance instanceIP dataFormatted request status userAgent longUrl
        // Type shortURL responseType responseURL userAgent bytesReturned redirectURL clientIP longURL
        var matchArray = line.match(re);
        return matchArray ? formatDataForLog(matchArray) : null;
    }
});

// var impressionToSQL = new parseToSQL({
//     files_path: path.resolve(__dirname, 'logs'),
//     file_type: "CDT",
//     db_name: "AccessLog2014071702",
//     create_statement: "CREATE TABLE IF NOT EXISTS impressions (instanceUUID TEXT, timestamp TEXT, detail TEXT, domain TEXT, browserName TEXT, platformName TEXT, osName TEXT, userAgent TEXT, city TEXT, clientTimestamp TEXT, registrationUUID TEXT)",
//     insert_statement: "INSERT INTO impressions VALUES ($instanceUUID, $timestamp, $detail, $domain, $browserName, $platformName, $osName, $userAgent, $city, $clientTimestamp, $registrationUUID)",
//     parseData: function(line) {
//         var obj = JSON.parse(line);
//         return obj ? formatDataForImpressions(obj["e2Impression"]) : null;
//     }
// });

logToSQL.run();