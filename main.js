// step 1: load needed libraries and modules
const express = require('express');
const morgan = require('morgan');
const mysql = require('mysql2/promise');
const { MongoClient, ObjectId } = require('mongodb');
const e = require('express');

// step 2: configure PORT
const PORT = parseInt(process.argv[2]) || parseInt(process.env.PORT) || 3000;

// step 3: create an instance of the express server
const app = express();

// step 4: create a connection pool to the SQL server and the Mongo server
const pool = mysql.createPool({
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT) || 3306,
    user: process.env.DB_USER,
    password: process.env.DB_PW,
    database: process.env.DB_NAME || 'bgg',
    connectionLimit: process.env.DB_CONN_LIMIT || 4,
    timezone: '+08:00'
});

const MONGO_URL = process.env.MG_HOST || 'mongodb://localhost:27017';
const MONGO_DB_NAME = process.env.MG_NAME || 'bgg';
const MONGO_COLLECTION_NAME = process.env.MG_COLLECTION || 'games';

const client = new MongoClient(MONGO_URL, {
    useNewUrlParser: true,
    useUnifiedTopology: true
});

// step 5: write a start program to ensure that a connection to the databases can be established
const startApp = async (newApp, newPool, newClient) => {
    // we still do not know if a connection can be established
    try {
        const conn = await newPool.getConnection();

        console.info("=> Pinging MySQL database..");
        const p0 = conn.ping();
        console.info("=> Connecting to MongoDB..");
        const p1 = newClient.connect();

        Promise.all([p0, p1])
            .then(() => {
                conn.release();
                // newClient.close();
                newApp.listen(PORT, () => {
                    console.info(`We are starting server at port ${PORT} on ${new Date()}`);
                });
            }).catch(e => {
                console.error('=> Unable to establish connection to DB: ', e);
            });
    } catch (e) {
        console.error("=> Unable to establish connection to DB: ", e);
    };
};

/* alternatively, we can use IIFE (immediately invoked function execution)
const p0 = (async () => {
    const conn = await = pool.getConnection();
    await conn.ping();
    conn.release();
    return true;
}) (); <- note: the call of the function with the parenthesis at the end

const p1 = (async () => {
    await client.connect();
    return true;
}) ();

Promise.all([p0, p1])
    .then(result => {
        app.listen(PORT, () => {
            console.info(`Server started at port ${PORT} on ${new Date()}`);
        });
    });
End of alternative implementation with IIFE */

// step 6: define any required database queries
const SQL_QUERY_GAME_DETAILS = "select * from game where gid like ?";

const makeQuery = (sql, dbPool) => {
    console.info('=> Creating query: ', sql);
    return (async (args) => {
        const conn = await dbPool.getConnection();
        try {
            let results = await conn.query(sql, args) || [];
            return results[0];
        } catch (e) {
            console.error(`=> ${sql} error: `, e);
        } finally {
            conn.release();
        }
    });
}

const queryGameDetails = makeQuery(SQL_QUERY_GAME_DETAILS, pool);

const mgGameReviews = async (gameId, dbClient) => {
    return await dbClient.db(MONGO_DB_NAME).collection(MONGO_COLLECTION_NAME)
        .aggregate([
            {
                '$match': {
                    'ID': gameId
                }
            },
            {
                '$limit': 50
            },
            {
                '$group': {
                    '_id': '$ID',
                    'reviews': {
                        '$push': '$comment'
                    },
                    'average_rating': {
                        '$avg': '$rating'
                    }
                }
            }
        ]).toArray();
};

// step 7: define any middleware or routes
app.use(morgan('combined'));

// GET /game/:id
// returns { name:, year:, url:, image:, reviews:[...], average_rating:}
app.get('/game/:id', async (req, res, next) => {
    const gameId = req.params['id'];

    console.info('=> getting gameDetails with gameId: ', gameId);
    const gameDetails = await queryGameDetails([ `${gameId}` ]);
    console.info('=> getting gameReviews with gameId: ', gameId);
    const gameReviews = await mgGameReviews(parseInt(gameId), client);

    // console.info('=> gameReviews: ', JSON.stringify(gameReviews));
    
    res.status(200).contentType('application/json').json({ gameDetails: gameDetails, gameReviews: gameReviews });
});

// step 8: start the express server
startApp(app, pool, client);
