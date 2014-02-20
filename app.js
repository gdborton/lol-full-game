var mongo = require('mongojs');

var collections = ['champions', 'summoners', 'games', 'gamePlayerStats'];
var dburl = process.env.DB_URL;
var db = mongo(dburl, collections);

function map(){
    emit(this.gameId, 1);
}

function reduce(key, values){
    return Array.sum(values);
}

var currentID;
var total = 0;

function checkNextBatch() {
    db.gamePlayerStats.mapReduce(
        map,
        reduce,
        {
            query: currentID ? { gameId : { $gt: currentID }} : {},
            limit: 10000,
            sort: {
                gameId: 1
            },
            out: {
                inline: 1
            }
        },
        function(err, what){
            if (what.length > 0){
                what.forEach(function(element) {
                    if (element.value > 9 ) {
                        console.log(element);
                        total++;
                    }
                });
                currentID = what[what.length-1]._id;
            } else {
                clearInterval(intervalID);
                process.exit();
            }
        }
    );
    console.log('total', total);
}

var intervalID = setInterval(checkNextBatch, 2000);