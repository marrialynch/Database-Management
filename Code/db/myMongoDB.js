const { MongoClient } = require("mongodb");
const { createClient } = require("redis");

async function getGames(query, page, pageSize) {
  let db, client, result;
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();

    console.log("Connected to Mongo Server");

    db = client.db("olympic_games");
    const gamesCollection = db.collection("games");

    // MQL 👉 json
    let q = {};
    let m = {$match:{"city" : new RegExp("^" + query, "i") }};
    let s = {$skip : (page - 1) * 10};
    let l = {$limit: pageSize};
    q = [m, s, l];
    result = await gamesCollection.aggregate(q).toArray();
    return result;
  } finally {
    await client.close();
    console.log("getGames() : result", result);
  }
}

async function getGamesCount(query) {
  let db, client, result;
  
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
  
    console.log("Connected to Mongo Server");
  
    db = client.db("olympic_games");
    const gamesCollection = db.collection("games");
  
    // MQL 👉 json
    const q = {"city" : new RegExp("^" + query, "i") };
    console.log("getGamesCount() : query", q);
    result = gamesCollection.find(q).count();
    return await result;
  } finally {
    await client.close();
  }
}

async function getSports(query, page, pageSize) {
  let db, client, result;
    
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
    
    console.log("Connected to Mongo Server");
    
    db = client.db("olympic_games");
    const sportsCollection = db.collection("sports");
    
    // MQL 👉 json
    let q = {};
    let m = {$match :{"sportsType" : new RegExp("^" + query, "i") }};
    let s = {$skip : (page - 1) * 10};
    let l = {$limit: pageSize};
    q = [m, s, l];
    console.log("getSports() : query", q);
    result = await sportsCollection.aggregate(q).toArray();
    return result;
  } finally {
    await client.close();
  }
}

async function getSportsCount(query) {
  let db, client, result;
      
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
      
    console.log("Connected to Mongo Server");
      
    db = client.db("olympic_games");
    const sportsCollection = db.collection("sports");
      
    // MQL 👉 json
    const q = {"sportsType" : new RegExp("^" + query, "i") };
    console.log("getSports() : query", q);
    result = sportsCollection.find(q).count();
    return await result;
  } finally {
    await client.close();
  }
}

async function getEvents(query, page, pageSize) {
  let db, client, result;
      
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
      
    console.log("Connected to Mongo Server");
    
    db = client.db("olympic_games");
    const sportsCollection = db.collection("sports");
      
    // MQL 👉 json
    let q = {};
    console.log("getEvents() : query", q);
    let m =  { $match:{"events.eventType":  new RegExp("^" + query, "i")}};
    let uw =  { $unwind : "$events" };
    let g = { $group: { _id: "$events.eventType"}  };
    let m2 = { $match : {"events.eventType": new RegExp("^" + query, "i")}};
    let p = { $project : { _id :0, eventType : "$_id"} };
    let s = {$skip : (page - 1) * 10};
    let l = {$limit: pageSize};
    q = [m, uw, m2, g, p, s, l ];
    result = await sportsCollection.aggregate(q).toArray();
    return  result;
  } finally {
    await client.close();
    console.log("getEvents", result.length);
  }
}
  
async function getEventsCount(query) {
  let db, client, result;
        
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
        
    console.log("Connected to Mongo Server");
        
    db = client.db("olympic_games");
    const sportsCollection = db.collection("sports");
        
    // MQL 👉 json
    let q = {};
    console.log("getEvents() : query", q);
    let m =  { $match:{"events.eventType":  new RegExp("^" + query, "i")}};
    let uw =  { $unwind : "$events" };
    let g = { $group: { _id: "$events.eventType"}  };
    let m2 = { $match : {"events.eventType": new RegExp("^" + query, "i")}};
    let p = { $project : { _id :0, eventType : "$_id"} };
    q = [m, uw, m2, g, p ];
    result = (await sportsCollection.aggregate(q).toArray()).length;
    return result;
  } finally {
    await client.close();
    console.log("getEventsCount", result);
  }
}

async function getEventsBySport(query, page, pageSize) {
  let db, client, result;
        
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
        
    console.log("Connected to Mongo Server");
        
    db = client.db("olympic_games");
    const sportsCollection = db.collection("sports");
        
    // MQL 👉 json
    let q = {};
    let m =  { $match:{"sportsType":  new RegExp("^" + query, "i")}};
    let uw =  { $unwind : "$events" };
    let g = { $group: { _id: "$events.eventType"}  };
    let s = {$skip : (page - 1) * 10};
    let l = {$limit: pageSize};
    let p = { $project : { _id :0, eventType : "$_id"} };
    q = [m, uw, g, s, l, p ];// 
    console.log("q", q);
    result = await sportsCollection.aggregate(q).toArray();
    return  result;
  } finally {
    await client.close();
    console.log("getEventsBySport",result.length);
  }
}
    
    
async function getEventsBySportCount(query) {
  let db, client, result;
          
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
          
    console.log("Connected to Mongo Server");
          
    db = client.db("olympic_games");
    const sportsCollection = db.collection("sports");
          
    // MQL 👉 json
    let q = {};
    let m =  { $match:{"sportsType":  new RegExp("^" + query, "i")}};
    let uw =  { $unwind : "$events" };
    let g = { $group: { _id: "$events.eventType"}  };
    let p = { $project : { _id :0, eventType : "$_id"} };
    q = [m, uw,  g, p ];
    result =  await sportsCollection.aggregate(q).toArray();
    return  result.length;
  } finally {
    await client.close();
  }
}

async function getGenderStatisticsBySportType(sportId) {
  let db, client, result;
          
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
          
    console.log("Connected to Mongo Server");
          
    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");
          
    // MQL 👉 json
    let q = {};
    sportId = parseInt(sportId);
    console.log("sportId",sportId);
    let m =  { $match: {"participations.sportId":  sportId }};
    let g = { $group: { _id: "$sex", count : {$sum : 1}}};
    let p = { $project : { _id :0, sex : "$_id", count : 1} };
    q = [m, g , p];// 
    console.log("q", q);
    result = await athletesCollection.aggregate(q).toArray();
    return  result;
  } finally {
    await client.close();
    console.log("getEventsBySport", result);
  }
}

async function getAthletes(query, page, pageSize) {
  let db, client, result;

  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
      
    console.log("Connected to Mongo Server");
    
    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");
      
    // MQL 👉 json
    let q = {};
    console.log("getAthletes() : query", q);
    let m = {$match :{"name" : new RegExp("^" + query, "i") }};
    let s = {$skip : (page - 1) * 10};
    let l = {$limit: pageSize};
    q = [m, s, l];
    result = await athletesCollection.aggregate(q).toArray();
    return result;
  } finally {
    await client.close();
  }
}

async function getAthletesCount(query) {
  let db, client, result;
    
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
      
    console.log("Connected to Mongo Server");
      
    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");
      
    // MQL 👉 json
    const q = {"name" : new RegExp("^" + query, "i") };
    console.log("getAthletes() : query", q);
    result = athletesCollection.find(q).count();
    return await result;
  } finally {
    await client.close();
  }
}

async function getAthleteByID(query) {
  let db, client, result;
    
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
      
    console.log("Connected to Mongo Server");
      
    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");
      
    // MQL 👉 json
    const q =  {"athleteId":  query};
    console.log("getAthleteById() : query", q);
    result = athletesCollection.find(q);
    console.log("getAthleteById result: " + JSON.stringify(result));
    return result;
  } finally {
    await client.close();
  }
}

async function getGamesByAthleteID(query) {
  let db, client, result;
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();

    console.log("Connected to Mongo Server");

    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");

    // MQL 👉 json
    let q = {};
    let q_int = parseInt(query);
    let m =  { $match:{"athleteId": q_int}};
    let lu = { $lookup:{"from": "games", "localField": "participations.gameId", "foreignField": "gameId", "as": "games_participated"}};
    let uw = { $unwind : "$games_participated" };
    let r = { $replaceRoot: {"newRoot": "$games_participated"}};
    let p = { $project : { _id :0, "year": 1, "season": 1, "city": 1} };
    q = [m, lu, uw, r, p];
    result = await athletesCollection.aggregate(q).toArray();
    console.log("getGamesByAthleteID() : result", result);
    return  result;
  } finally {
    await client.close();
  }
}

async function updateAthletesByID(query,ref) {
  let db, client, result;
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();

    console.log("Connected to Mongo Server");

    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");

    // MQL 👉 json
    let q_int = parseInt(query);
    //---------------------------------------------------
    let q =  { "athleteId":  q_int, };
    //https://pretagteam.com/question/nodejs-mongoose-update-nested-array-with-all-values-in-reqbody
    console.log("new name: " + ref.name);
    console.log("new weight: " + ref.weight);
    let newValues = {$set: {"name": ref.name, "participations.$.weight": ref.weight, "participations.$.age": ref.age}};
    // let filter = {arrayFilters:[{"outer._id": 0}]};

    result = await athletesCollection.updateOne(q, newValues);
    console.log("updateAthletesByID() : result", JSON.stringify(result));
    return  result;
  } finally {
    await client.close();

  }
}

async function deleteAthletesByID(query) {
  let db, client, result;
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();

    console.log("Connected to Mongo Server");

    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");

    // MQL 👉 json
    let q =  { "athleteId":  new RegExp("^" + query, "i")};
    result = await athletesCollection.deleteOne(q);
    return  result;
  } finally {
    await client.close();
    console.log("updateAthletesByID() : result", JSON.stringify(result));
  }
}

async function createAthlete(ref) {
  let db, client;
  try {
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();

    console.log("Connected to Mongo Server");

    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");

    // MQL 👉 json
    let athleteId = await athletesCollection.find({}).count() + 1;
    let gameId = await getGameId(ref, db);
    let teamId = await getTeamId(ref, db, gameId);

    let q =  {"athleteId": athleteId,"name": ref.name, "sex": ref.sex,"participations":{"gameId": gameId, "teamId": teamId, "age": ref.age, "height": ref.height, "weight": ref.weight}};
    await athletesCollection.insertOne(q);
  } finally {
    await client.close();
    console.log("createAthlete() : done");
  }
}

async function getGameId(ref, db){
  const gamesCollection = db.collection("games");
  let f = {"season": ref.season, "year": ref.year};
  let p = {_id: 0, gameId: 1};
  let result = gamesCollection.find(f, p).toArray()[0].gameId;
  console.log("gameId: " + result);
  return result;
}

async function getTeamId(ref, db, gameId){
  const gamesCollection = db.collection("games");
  let q = [];
  let m = {$match: {"gameId": gameId}};
  let uw = {$unwind: {"path": "$teams"}};
  let rr = {$replaceRoot: {"newRoot": "$teams"}}; 
  let m2 = {$match: {"country": ref.country}}; 
  let p = {$project: {"teamId": 1}};
  
  q = [m, uw, rr, m2, p];
  let result = await gamesCollection.aggregate(q).toArray();
  let str = "";
  result.forEach(function(item){
    if (item != null) {
      str = item.id;
      console.log("teamId:" + str);
      return str;
    }
  });
}

async function populateRedis(query) {
  let db, client, result, redisClient;
  try {
  // connect to mongodb
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
    console.log("Connected to Mongo Server");
    
    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");
    const gamesCollection = db.collection("games");

    // connect to Redis
    redisClient = createClient();
    redisClient.on("error", (err) => console.log("Redis Client Error", err));
    await redisClient.connect();
    console.log("Redis connected");
    await redisClient.flushAll();


    //find the top users by name and unwind participation
    const pipleline = [
      {
        "$match": {
          "name": { // ------------------注意在router改！！！！
            "$regex": new RegExp(query)
          }
        }
      }, {
        "$unwind": {
          "path": "$participations"
        }
      }
    ];

    result = await athletesCollection.aggregate(pipleline).toArray();
    console.log("result from mongo: " + result);
    
    // get each participation by gameId and store it in hash if it's not already in
    await Promise.all(result.map(async (item) => {
      try{
        if (item != null) {
          const user_name = item.name;
          console.log("user name: " + user_name);
          const game_id = item.participations.gameId;
          console.log("gameId: " + game_id);
          // use cache in cache to get game year and season
          //check if gameId exists in hash
          const check_res = await redisClient.exists(`${game_id}`);
          if(check_res) {
            console.log("exist");
            const game_year = await redisClient.hGet(`${game_id}`, "year");
            const game_season = await redisClient.hGet(`${game_id}`, "season");
            await redisClient.ZADD(`${user_name}`,{score: `${game_year}`, value: `${game_year} ${game_season}`});
          } else {
            console.log(`${game_id} not exist`);
            // only need the year and season, so findOne
            const game_cursor = await gamesCollection.findOne({"gameId": game_id}, {projection: {_id:0, year: 1, season:1}});
            console.log(JSON.stringify(game_cursor));
            console.log("finish finding in mongo");
            const game_year = game_cursor.year;
            console.log(game_year);
            const game_season = game_cursor.season;
            console.log(game_season);
            // console.log("successfully get game year and season from mongo.games, now proceed to store into Redis");
            await redisClient.hSet(`${game_id}`, {"season": game_season, "year": game_year});
            // console.log("Finish set gameId in Redis");

            await redisClient.ZADD(`${user_name}`,{score: `${game_year}`, value: `${game_year} ${game_season}`});
            await redisClient.set(`${game_year} ${game_season}`, `${game_id}`);
            // console.log(`Finishe inserting user:${user_name}'s participation in ${game_year}`);
          }
            
          // const participation_list = await redisClient.zRangeWithScores("Chieko Nakanishi", 0, -1);
          // console.log(participation_list);
        } 
      } catch (error) {
        console.log(error);
      }
    }));
  } catch (error) {
    console.log(`caught - ${error}`);
    throw error;
  } finally{
    await client.close();
    await redisClient.quit();
  } 
}

async function getParticipations(query) {
  let redisClient;
  try {
    // connect to Redis
    redisClient = createClient();
    redisClient.on("error", (err) => console.log("Redis Client Error", err));
    await redisClient.connect();
    console.log("Redis connected");
    // await redisClient.flushAll();

    //--------------这里要改query的格式！
    await populateRedis(query);

    const key_res = await redisClient.keys(`*${query}*`);
    const res_list = [];
    // console.log(typeof(key_res)); // object with string
    console.log(key_res);

    await Promise.all(key_res.map(async (key) => {
      //check if it exists in redis
      const check_res = await redisClient.exists(key);
      console.log(`check res for ${key}: ${check_res}`);
      if (!check_res){
        // create new in redis
        await populateRedis(query);
      } 
      // get result and add to list
      const query_res =  await redisClient.zRangeWithScores(key, 0, -1);
      console.log(query_res); 
      // ------------要在这里把名字加进去吗？？？？？？？？
      query_res.push({name: `${key}`});
      res_list.push(query_res);
    // console.log(typeof(query_res)); // ----------list of object
    // res_list.push(...query_res);// 加list
    // {athlete_name: [{}, {}]}
    // [athleteName, [query_res]]
    }));
    //-------------------------返回类型现在是list of 【objects】！！！！！！！！
    //res_list: [[{"value":"1996 Summer","score":1996}],[{"value":"2004 Summer","score":2004},{"value":"2008 Summer","score":2008},{"name":"Wu Wen-Chien"}],[{"value":"1984 Summer","score":1984},{"value":"1988 Summer","score":1988},{"name":"Chang Hui-Chien"}]]
    console.log("res_list: " + JSON.stringify(res_list));
    return res_list;
  } catch (error) {
    console.log(`caught - ${error}`);
    throw error;
  } finally{
    await redisClient.quit();
  }  
}

async function updateNameInParticipations(old_name, new_name) {
  let db, client, redisClient;
  try {
    // connect to mongodb
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
    console.log("Connected to Mongo Server");
    
    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");

    // connect to Redis
    redisClient = createClient();
    redisClient.on("error", (err) => console.log("Redis Client Error", err));
    await redisClient.connect();
    console.log("Redis connected");

    // update in redis
    const r_rds = await redisClient.rename(`${old_name}`, `${new_name}`);
    console.log("update name in redis: " + r_rds);

    //update name in mongodb
    const r_mongo = await athletesCollection.updateOne(
      {"name": `${old_name}`}, 
      {"$set": 
        {
          "name": `${new_name}`
        }
      });
    console.log("update name in mongo: " + JSON.stringify(r_mongo));
  } catch (error) {
    console.log(`caught - ${error}`);
    throw error;
  } finally{
    await client.close();
    await redisClient.quit();
  }  
}

//-----------------返回类型是什么？？line 44
async function deleteParticipations(name, game_year, game_season) {
  let db, client, redisClient;
  try {
    // connect to mongodb
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
    console.log("Connected to Mongo Server");
    
    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");
    // const gamesCollection = db.collection("games");

    // connect to Redis
    redisClient = createClient();
    redisClient.on("error", (err) => console.log("Redis Client Error", err));
    await redisClient.connect();
    console.log("Redis connected");
    // await redisClient.flushAll();


    // delete in redis
    const r_rds = await redisClient.zRem(`${name}`, `${game_year} ${game_season}`); // 成功返回1，否则返回0
    console.log("result: " + r_rds);

    // get gameId in redis
    const game_id = await redisClient.get(`${game_year} ${game_season}`);
    console.log("type of gameId: " + game_id);
    console.log("gameId: " + game_id);

    //delete in mongodb
    const r_mongo = await athletesCollection.updateOne(
      {"name": `${name}`}, 
      {"$pull": {
        "participations": {
          "gameId": game_id
        }}
      }
    );
    console.log("result for removing in mongodb: " + JSON.stringify(r_mongo));

  } catch (error) {
    console.log(`caught - ${error}`);
    throw error;
  } finally{
    await client.close();
    await redisClient.quit();
  }  
}

//-----------------返回类型是什么？？line29行改
async function createParticipations(name, game_year, game_season) {
  let db, client, redisClient, game_id;
  try {
    // connect to mongodb
    const uri = "mongodb://localhost:27017";
    client = new MongoClient(uri);
    await client.connect();
    console.log("Connected to Mongo Server");
    
    db = client.db("olympic_games");
    const athletesCollection = db.collection("athletes");
    const gamesCollection = db.collection("games");

    // connect to Redis
    redisClient = createClient();
    redisClient.on("error", (err) => console.log("Redis Client Error", err));
    await redisClient.connect();
    console.log("Redis connected");
    // await redisClient.flushAll();

    // create new participation in redis
    const r_rds = await redisClient.ZADD(`${name}`,{score: `${game_year}`, value: `${game_year} ${game_season}`});
    console.log("result for updating redis: " + r_rds);

    // check if gameId in redis
    const check_res = await redisClient.exists(`${game_id}`);
    if(check_res) {
      // get gameId in redis
      game_id = await redisClient.get(`${game_year} ${game_season}`);
    } else {
      // if gameId is not in redis, get gameId in mongodb and add it to redis
      const game_cursor = await gamesCollection.findOne({"year": game_year}, {projection: {_id:0, gameId: 1}});
      console.log(game_cursor);
      game_id = game_cursor.gameId;
    }
    console.log("gameId: " + game_id);

    // insert new participation with game_id in mongodb
    const r_mongo = await athletesCollection.findOneAndUpdate(
      {name: `${name}`},
      {$push: 
        { participations: 
          {
            gameId: game_id
          }
        }
      },
      {upsert: true, returnOriginal: false }
    );
    console.log(r_mongo);
    return r_mongo.lastErrorObject.updatedExisting;  // this is a boolean!
  } catch (error) {
    console.log(`caught - ${error}`);
    throw error;
  } finally{
    await client.close();
    await redisClient.quit();
  }  
}

module.exports.getGames = getGames;
module.exports.getGamesCount = getGamesCount;
module.exports.getSports = getSports;
module.exports.getSportsCount = getSportsCount;
module.exports.getEvents = getEvents;
module.exports.getEventsCount = getEventsCount;
module.exports.getEventsBySport = getEventsBySport;
module.exports.getEventsBySportCount = getEventsBySportCount;
module.exports.getGenderStatisticsBySportType = getGenderStatisticsBySportType;
module.exports.getAthletes = getAthletes;
module.exports.getAthletesCount = getAthletesCount;
module.exports.getAthleteByID = getAthleteByID;
module.exports.getGamesByAthleteID = getGamesByAthleteID;
module.exports.updateAthletesByID = updateAthletesByID;
module.exports.deleteAthletesByID = deleteAthletesByID;
module.exports.createAthlete = createAthlete;
// the following 4 functions are newly added for project 3
module.exports.getParticipations = getParticipations;
module.exports.updateNameInParticipations = updateNameInParticipations;
module.exports.deleteParticipations = deleteParticipations;
module.exports.createParticipations = createParticipations;