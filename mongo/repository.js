/**
 * @param client
 * @constructor
 */
function MongoDbRepository (client) {
    this.client = client;
    this.errors = [];
}

MongoDbRepository.prototype = {

    insert: function (document, collection, callback) {
        this.client.getConnection(function(err, db) {
            if (err) {
                callback(err, false);
            } else {
                db.collection(collection)
                    .insertOne(document, function(err) {
                        db.close();
                        if (err) {
                            callback(err, false);
                        } else {
                            callback(null, true);
                        }
                    });
            }
        });
    },

    insertMany: function (documents, collection, callback) {
        this.client.getConnection(function(err, db) {
            if (err) {
                callback(err, false);
            } else {
                db.collection(collection)
                    .insertMany(documents, function(err) {
                        db.close();
                        if (err) {
                            callback(err, false);
                        } else {
                            callback(null, true);
                        }
                    });
            }
        });
    },

    update: function(document, collection, key, value, callback) {
        var query = {};
        query[key] = value;
        this.client.getConnection(function(err, db) {
            if (err) {
                console.log(err);
                callback(err, null);
            } else {
                db.collection(collection)
                    .findOneAndUpdate(query, document, function(err, d) {
                        db.close();
                        if (err) {
                            console.log(err);
                            callback(err, null);
                        }
                        callback(null, d);
                    });
            }

        });
    },

    find: function (collection, query, skip, limit, callback) {
        this.client.getConnection(function(err, db) {
            if (err) {
                console.log(err);
                callback(err, null);
            } else {
                db.collection(collection)
                    .find(query)
                    .skip(skip)
                    .limit(limit)
                    .toArray(function(err, d) {
                        db.close();
                        if (err) {
                            console.log(err);
                            callback(err, null);
                        }
                        callback(null, d);
                    });
            }
        })
    },

    sortedFind: function (collection, query, skip, limit, sort, callback) {
        this.client.getConnection(function(err, db) {
            if (err) {
                console.log(err);
                callback(err, null);
            } else {
                db.collection(collection)
                    .find(query)
                    .skip(skip)
                    .limit(limit)
                    .sort(sort)
                    .toArray(function(err, d) {
                        db.close();
                        if (err) {
                            console.log(err);
                            callback(err, null);
                        }
                        callback(null, d);
                    });
            }
        })
    },

    count: function(collection, query, callback) {
        this.client.getConnection(function(err, db) {
            if (err) {
                console.log(err);
                callback(err, null);
            } else {
                db.collection(collection)
                    .count(query, function(err, d) {
                        db.close();
                        if (err) {
                            console.log(err);
                            callback(err, null);
                        }
                        callback(null, d);
                    });
            }
        })
    },

    dbList: function (callback) {
        this.client.getConnection(function(err, db) {
            var adminDb = db.admin();
            adminDb.listDatabases().then(function(dbs) {
                db.close();
                callback(dbs);
            });
        });
    },

    truncate: function(collection, callback) {
        this.client.getConnection(function(err, db) {
            if (err) {
                console.log(err);
                callback(err, null);
            } else {
                db.collection(collection)
                    .deleteMany({}, function(err, d) {
                        db.close();
                        if (err) {
                            console.log(err);
                            callback(err, null);
                        }
                        callback(null, d);
                    });
            }
        })
    }

};

module.exports = MongoDbRepository;