var uuid = require('node-uuid');
var mysql = require('mysql');

var pool = mysql.createPool({
    host: 'crowdflik-db-dev.cbuzk3devevt.us-east-1.rds.amazonaws.com',
    user: 'crowdflik',
    password: 'applerules'
});
/**
 * POST Method for creating an Event.
 *
 * @param req
 * @param res
 */
exports.createEvent = function createEvent(req, res) {

    var event = req.body;
    if(event.id!==null){
        res.send(400, "Event with id " + event.id + " already exists!");
    }

    //Set id, created and modified times
    var time = getCurrentUtcTimeInMillis();
    event.id = uuid.v1();
    event.created = time;
    event.modified = time;
    event.distanceInMiles = null;
    event.isPublic = event.isPublic == "true" ? 1 : 0;

    pool.getConnection(function (err, connection) {
        //Insert new event into database
        connection.query(generateQuery(event), function (err, rows) {
            connection.end(); // Don't use the connection after here, it has been returned to the pool.
            //If there was a SQL error
            if(err){
                res.send(500, err);
            }
            res.send(event);
        });
    });
};

/**
 * GET method that returns all events
 *
 * @param req
 * @param res
 */
exports.events = function listEvents(req, res) {
    pool.getConnection(function (err, connection) {
        connection.query('SELECT * FROM crowdflik.EVENTS', function (err, rows) {
            connection.end(); // Don't use the connection here, it has been returned to the pool.
            res.send(JSON.stringify(rows));
        });
    });
};

/**
 * GET method that returns data for a specified eventId passed in my a path variable.
 *
 * @param req
 * @param res
 */
exports.event = function getEvent(req, res) {
    var eventId = req.params.eventId; //Path Variable

    pool.getConnection(function (err, connection) {
        connection.query('SELECT * FROM crowdflik.EVENTS WHERE id = \'' + eventId + '\'', function (err, rows) {

            connection.end(); // Don't use the connection after here since it has been returned to the pool.
            res.send(JSON.stringify(rows));
        });
    });
};

/**
 * Calls a Stored Procedure to determine events created in the last 24 hours within a 5 mile radius of their current
 * location.
 *
 * @param req
 * @param res
 */
exports.findEventNearYou = function findEventsNearYou(req, res) {
    var latitude = req.params.latitude;
    var longitude = req.params.longitude;
    var radialDistanceInMiles = 5;
    var startTime = getCrowdflikEventUtcStartTimeInMillis();

        pool.getConnection(function (err, connection) {
            connection.query('CALL crowdflik.FindEventsNearUserLocationWithinRadialDistanceInMiles(' + latitude + ','
                + longitude + ',' + radialDistanceInMiles + ',' + startTime + ')', function(err, rows) {

                connection.end(); // Don't use the connection after here since it has been returned to the pool.
                res.send(JSON.stringify(rows));
            });
        });

};

/**
 * Returns the Current UTC time minus 24 hours in milliseconds.
 *
 * @return {Number}
 */
function getCrowdflikEventUtcStartTimeInMillis(){
    var now = new Date();
    now.setHours(now.getHours()-24);
    var now_utc = new Date(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(),  now.getUTCHours(),
        now.getUTCMinutes(), now.getUTCSeconds());
    return now_utc.getTime();
}

/**
 * Returns the current UTC system time in milliseconds.
 *
 * @return {Number}
 */
function getCurrentUtcTimeInMillis(){
    var now = new Date();
    var now_utc = new Date(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(),  now.getUTCHours(),
        now.getUTCMinutes(), now.getUTCSeconds());
    return now_utc.getTime();
}

/**
 * Generates a query from a passed in Event using array push and join for improved performance over plain string
 * concatenation.
 *
 * @param event
 * @return {String}
 */
function generateQuery(event) {
    var query = [];
    query.push('INSERT INTO crowdflik.EVENTS (id,title,isPublic,latitude,longitude,horizontalAccuracy,distanceInMiles,created,modified) ');
    query.push('VALUES (\'');
    query.push(event.id);
    query.push('\',');
    query.push('\'');
    query.push(event.title);
    query.push('\',');
    query.push(event.isPublic);
    query.push(',');
    query.push(event.latitude);
    query.push(',');
    query.push(event.longitude);
    query.push(',');
    query.push(event.horizontalAccuracy);
    query.push(',null,');
    query.push(event.created);
    query.push(',');
    query.push(event.modified);
    query.push(')');

    return query.join('');
}