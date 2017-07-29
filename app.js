#!/usr/bin/node
/* jshint esnext:true */
"use strict";

const winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    Seq = require("sequelize"),
    elasticsearch = require("elasticsearch");

const DATABASE_URI = process.env.DATABASE_URI,
    ELASTIC_URI = process.env.ELASTIC_URI || "localhost:9200",
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    BATCHSIZE = process.env.BATCHSIZE || 50,
    MAXCONNS = parseInt(process.env.MAXCONNS) || 20;

const logger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({
            timestamp: true,
            colorize: true
        })
    ]
});

// loggly integration
if (LOGGLY_TOKEN)
    logger.add(winston.transports.Loggly, {
        inputToken: LOGGLY_TOKEN,
        subdomain: "kvahuja",
        tags: ["backend", "reaper"],
        json: true
    });


// connect to rabbit & db
const seq = new Seq(DATABASE_URI, {
        logging: false,
        max: MAXCONNS
    }),
    model = require("../orm/model")(seq, Seq),
    elastic = new elasticsearch.Client({ host: ELASTIC_URI, log: "info" });

// assumes `id` exists
// Sequelize model, index type, index key, key to parent id, hook, custom condition
async function load(table, type, includes, filter) {
    // load from biggest id to 0
    const last_id_r = await model.Keys.findOrCreate({
            where: { type: "reaper_last_id_fetched", key: type },
            defaults: { value: 2147483647 }
        });
    while (true) {
        const last_id = last_id_r[0].value,
            condition = Object.assign({}, filter,
                { id: { $lt: last_id } });

        logger.info("loading", { type, last_id });

        let data = await table.findAll({
            where: condition,
            order: [ [seq.col("id"), "DESC"] ],
            include: includes,
            limit: BATCHSIZE,
            raw: true
        });
        if (data.length == 0) break;  // exhausted
        await last_id_r[0].update({ value: data[data.length-1].id });

        await elastic.bulk({
            body: [].concat(... data.map((d) => [
                { index: {
                    _index: type,
                    _type: type,
                    _id: d.api_id || d.id
                } },
                d
            ]) )
        });
    }

    logger.info("done.", { type });
}

(async function() {
    await Promise.all([
        load(model.Participant, "participant", [
            model.ParticipantStats,
            model.Player,
            model.Roster,
            model.Match,

            model.Region, model.Hero, model.Series, model.GameMode, model.Role
        ]),
        load(model.ParticipantPhases, "participant_phases", [ {
            model: model.Participant,
            include: [
                model.Region, model.Hero, model.Series, model.GameMode, model.Role
            ]
        } ]),
        //load(model.Match, "match", [ model.Asset ]),

        load(model.PlayerPoint, "player_point", [ {
            model: model.Player,
            include: [ model.Region ]
        },
            model.Series, model.Hero, model.GameMode, model.Role
        ]),
        //load(model.Player, "player", [ model.Region ]),
    ]);
})();

process.on("unhandledRejection", (err) => {
    logger.error(err);
    //process.exit();
});
