const express = require("express");
const router = express.Router();

const myDb = require("../db/myMongoDB.js");

/* GET home page. */
router.get("/", async function (req, res) {
  res.redirect("/games");
});

// http://localhost:3000/references?pageSize=24&page=3&q=John
router.get("/games", async (req, res, next) => {
  const query = req.query.q || "";
  const page = +req.query.page || 1;
  const pageSize = +req.query.pageSize || 24;
  const msg = req.query.msg || null;
  try {
    let total = await myDb.getGamesCount(query);
    let games = await myDb.getGames(query, page, pageSize);
    let athletes = null;
    let sports = null;
    let events = null;
    let participations = null;
    res.render("./pages/index", {
      games,
      athletes,
      sports,
      events,
      participations,
      query,
      msg,
      currentPage: page,
      lastPage: Math.ceil(total/pageSize),
    });
  } catch (err) {
    next(err);
  }
});


router.get("/athletes", async (req, res, next) => {
  const query = req.query.q || "";
  const page = +req.query.page || 1;
  const pageSize = +req.query.pageSize || 24;
  const msg = req.query.msg || null;
  try {
    let total = await myDb.getAthletesCount(query);
    let athletes = await myDb.getAthletes(query, page, pageSize);
    let games = null;
    let sports = null;
    let events = null;
    res.render("./pages/index", {
      athletes,
      games,
      query,
      events,
      sports,
      msg,
      currentPage: page,
      lastPage: Math.ceil(total/pageSize),
    });
  } catch (err) {
    next(err);
  }
});


router.get("/sports", async (req, res, next) => {
  const query = req.query.q || "";
  const page = +req.query.page || 1;
  const pageSize = +req.query.pageSize || 24;
  const msg = req.query.msg || null;
  try {
    let total = await myDb.getSportsCount(query);
    let sports = await myDb.getSports(query, page, pageSize);
    let games = null;
    let athletes = null;
    let events = null;
    res.render("./pages/index", {
      sports,
      games,
      athletes,
      events,
      query,
      msg,
      currentPage: page,
      lastPage: Math.ceil(total/pageSize),
    });
  } catch (err) {
    next(err);
  }
});


router.get("/events", async (req, res, next) => {
  if(req.query.eventsBySportQuery){
    let query = req.query.eventsBySportQuery;
    const page = +req.query.page || 1;
    const pageSize = +req.query.pageSize || 24;
    const msg = req.query.msg || null;
    const eventsBySportQuery = query;
    console.log("/events eventsBySportQuery", eventsBySportQuery);
    query = "";
    let games = null;
    let athletes = null;
    try {
      let total = await myDb.getEventsBySportCount(eventsBySportQuery);
      let events = await myDb.getEventsBySport(eventsBySportQuery, page, pageSize);
      let sports = null;
      res.render("./pages/index", {
        sports,
        events,
        query,
        eventsBySportQuery,
        games,
        athletes,
        msg,
        currentPage: page,
        lastPage: Math.ceil(total/pageSize),
      });
    } catch (err) {
      next(err);
    }
  }
  else{
    let query = req.query.q || "";
    const page = +req.query.page || 1;
    const pageSize = +req.query.pageSize || 24;
    const msg = req.query.msg || null;
    console.log("/events query", query);
    try {
      let total = await myDb.getEventsCount(query);
      let events = await myDb.getEvents(query, page, pageSize);
      let games = null;
      let athletes = null;
      let sports = null;
      const eventsBySportQuery = "";
      res.render("./pages/index", {
        sports,
        events,
        games,
        athletes,
        query,
        eventsBySportQuery,
        msg,
        currentPage: page,
        lastPage: Math.ceil(total/pageSize),
      });
    } catch (err) {
      next(err);
    }
  }

});

router.get("/athletes/:athleteId/edit", async (req, res, next) => {
  const athleteID = req.params.athleteId;
  console.log("athleteId before passing in getAthByID: " + athleteID);
  const page = +req.query.page || 1;
  const pageSize = +req.query.pageSize || 24;
  const msg = req.query.msg || null;
  try {
    let athlete = await myDb.getAthleteByID(athleteID);
    let games = await myDb.getGamesByAthleteID(athleteID);
    console.log("edit atheletes", {
      athlete,
      games,
      msg,
    });
    const total = games.length;

    res.render("./pages/editAthlete", {
      athlete,
      games,
      msg,
      currentPage: page,
      lastPage: Math.ceil(total/pageSize),
    });
  } catch (err) {
    next(err);
  }
});

router.post("/athletes/:athleteId/edit", async (req, res, next) => {
  const athleteID = req.params.athleteId;
  const ref = req.body;

  try {

    let updateResult = await myDb.updateAthletesByID(athleteID, ref);
    console.log("update", updateResult);

    if (updateResult && updateResult.modifiedCount === 1) {
      res.redirect("/athletes/?msg=Updated");
    } else {
      res.redirect("/athletes/?msg=Error Updating");
    }

  } catch (err) {
    next(err);
  }
});

router.get("/athletes/:athleteId/delete", async (req, res, next) => {
  const athleteID = req.params.athleteId;

  try {

    let deleteResult = await myDb.deleteAthletesByID(athleteID);
    console.log("delete", deleteResult);

    if (deleteResult && deleteResult.deletedCount === 1) {
      res.redirect("/athletes/?msg=Deleted");
    } else {
      res.redirect("/athletes/?msg=Error Deleting");
    }

  } catch (err) {
    next(err);
  }
});

router.get("/sports/:sportsType/viewInfo", async (req, res, next) => {
  const sportId = req.query.sportId;
  const sportsType = req.params.sportsType;
  console.log("sportId",sportId, "sportsType",sportsType);

  try {

    let genderStats = await myDb.getGenderStatisticsBySportType(sportId);
    console.log("requested info", genderStats);

    if(genderStats.length === 0) {
      genderStats = [ { "sex": "F", "count": 0 }, { "sex": "M", "count": 0 } ];
    }

    res.render("./pages/genderStatistic", {
      sportsType,
      sportId,
      genderStats
    });


  } catch (err) {
    next(err);
  }
});

router.post("/createAthlete", async (req, res, next) => {
  const ref = req.body;
  console.log("medal", ref.medal);
  try {
    await myDb.createAthlete(ref);
    res.redirect("/athletes/?msg=New athlete created.");
    // const data = {};
    // res.status(200).send(data)
  } catch (err) {
    res.redirect("/athletes/?msg=Error creating athlete.");
    next(err);
  }
});

// show games info before search
router.get("/participations_home", async (req, res, next) => {
  const query = req.query.q || "";
  const page = +req.query.page || 1;
  const pageSize = +req.query.pageSize || 24;
  const msg = req.query.msg || null;
  try {
    let total = await myDb.getGamesCount(query);
    let athletes = null;
    let games = await myDb.getGames(query, page, pageSize);
    let participations = null;
    res.render("./pages/participation", {
      athletes,
      games,
      query,
      participations,
      msg,
      currentPage: page,
      lastPage: Math.ceil(total/pageSize),
    });
  } catch (err) {
    next(err);
  }
});

// display search by name result
router.get("/participations", async (req, res, next) => {
  const query = req.query.q || "";
  const msg = req.query.msg || null;
  try {
    let participations = myDb.getParticipations(query);
    res.render("./pages/participationsList", {
      participations,
      query,
      msg
    });
  } catch (err) {
    next(err);
  }
});

router.post("/participations/:athlete_p.name/edit", async (req, res, next) => {
  const athlete_name = req.params.athlete_p.name;
  try {
    let updateResult = await myDb.updateNameInParticipations(athlete_name);
    console.log("update", updateResult);

    if (updateResult === 1) {
      res.redirect("/participation/?msg=Updated");
    } else {
      res.redirect("/participation/?msg=Error Updating");
    }
  } catch (err) {
    next(err);
  }
});

router.get("/participations/:athlete_p.name/delete", async (req, res, next) => {
  const athlete_name = req.params.athlete_p.name;
  try {

    let deleteResult = await myDb.deleteParticipations(athlete_name);
    console.log("delete", deleteResult);

    if (deleteResult === 1) {
      res.redirect("/participation/?msg=Deleted");
    } else {
      res.redirect("/participation/?msg=Error Deleting");
    }

  } catch (err) {
    next(err);
  }
});

router.post("/createParticipation", async (req, res, next) => {
  const ref = req.body;
  let name = ref.athlete_p.name;
  let year = ref.year;
  let season = ref.season;
  try {
    const createResult = await myDb.createParticipations(name, year, season);
    if (createResult) {
      res.redirect("/participation/?msg=Created");
    } else {
      res.redirect("/participation/?msg=Error Creating");
    }
  } catch (err) {
    res.redirect("/athletes/?msg=Error creating athlete.");
    next(err);
  }
});

module.exports = router;
