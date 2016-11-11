/*global console, process, require, Promise */
require("longjohn");
var fs = require("fs");
var eventStream = require("event-stream");
var moment = require("moment");

var mongoDbConfig = {
    "host": "172.31.8.227",
    "port": "27017",
    "user": "",
    "pass": ""
};
var collectionName = 'log';
var dbName = 'test_db';

var mongoDbClient = require("./mongo/client");
var mongoDbRepository = require("./mongo/repository");
var mongoClient = new mongoDbClient(mongoDbConfig, dbName);
var mongo = new mongoDbRepository(mongoClient);


var numberOfRowsToSend = 0;
var regexGroups = {
    ip: 1,
    date: 4,
    method: 5,
    url: 6,
    httpCode: 7,
    userAgent: 10,
    anumber: 11,
    aresult: 12
};
var packageCounter = 0;
var lastCounterFile = "./lastCounter";
var lastCounter = parseInt(fs.readFileSync(lastCounterFile), 10);

var regEx = new RegExp('^([\\d.]+) (\\S+) (\\S+) \\[([\\w:\/]+\\s[+\-]\\d{4})\\] \"(\\S+) (.+?)\" (\\d{3}) (\\d+|-) \"([^\"]*)\" \"([^\"]*)\" (\\S+) (\\S+)');
var globalLine = null;
var rows = [];

function sendRows(rows) {
    "use strict";
    return new Promise(function (resolve, reject) {
        mongo.insertMany(rows, collectionName, function (error, status) {
            if (status) {
                resolve(status);
            } else {
                reject(error);
            }
        });
    });
}

var readStream = fs.createReadStream("varnish10gb.log")
    .pipe(eventStream.split())
    .pipe(eventStream.map(function (line) {
        "use strict";

        readStream.pause();
        globalLine = line;
        var matches = regEx.exec(line);

        if (matches && matches[regexGroups.date]) {
            if (packageCounter >= lastCounter) {
                var momentDate = moment(matches[regexGroups.date], "DD/MMM/YYYY:HH:mm:ss ZZ");
                var timestamp = momentDate.valueOf() / 1000;
                var data = {
                    "timestamp": timestamp,
                    "ip": matches[regexGroups.ip],
                    "url": matches[regexGroups.url],
                    "status_code": matches[regexGroups.httpCode],
                    "user_agent": matches[regexGroups.userAgent],
                    "a_number": matches[regexGroups.anumber],
                    "a_result": matches[regexGroups.aresult]
                };

                rows.push(data);
                numberOfRowsToSend += 1;

                if (numberOfRowsToSend === 500) {
                    console.log("limit reached, send");
                    sendRows(rows).then(function (response) {
                        console.log(packageCounter);
                        console.log(response);

                        packageCounter += 1;
                        fs.writeFileSync(lastCounterFile, packageCounter);
                        rows = [];
                        numberOfRowsToSend = 0;

                        readStream.resume();
                    }, function (error) {
                        console.log("error");
                        console.log(error);
                        process.exit(1);
                    });
                }
            } else {
                console.log("skipped", packageCounter);
                packageCounter += 1;

                readStream.resume();
            }
        }
    })
        .on("error", function (err) {
            "use strict";
            console.log("Error while reading file.");
            console.log(globalLine, err);
        })
        .on("end", function () {
            "use strict";
            console.log("Read entire file.");
        })
);
