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
async function load(table, type, parent_key, merger, filter) {
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

        let data = await table.findOne({
            where: condition,
            order: [ [seq.col("id"), "DESC"] ],
            raw: true
        });
        if (data == undefined) break;  // exhausted
        last_id_r[0].value = data.id;
        await last_id_r[0].save();

        // using a custom function, merge with other models as needed
        if (merger != undefined)
            // important! don't merge the other way round, merged ids should not overwrite
            data = Object.assign(await merger(data), data);

        let id = data.api_id != undefined? data.api_id:data.id,
            // pp doesn't have api id
            parent = parent_key != undefined? data[parent_key]:undefined;
        await elastic.create({
            index: "semc-vainglory",
            type,
            id,
            parent,
            body: data
        });
    }

    logger.info("done.", { type });
}

async function createParentChild(parent_type, child_type) {
    await elastic.indices.putMapping({
        index: "semc-vainglory",
        type: child_type,
        body: {
            "_parent": { "type": parent_type }
        }
    });
}

(async function() {
    try {
        await elastic.indices.create({ index: "semc-vainglory" });
        await Promise.all([
            createParentChild("participant", "participant_phases"),
            createParentChild("roster", "participant"),
            createParentChild("match", "roster"),
            createParentChild("player", "player_point")
        ]);
    } catch (e) { }  // already exist

    await Promise.all([
        load(model.Match, "match", undefined, async (m) =>
            await model.Asset.findOne({
                where: { match_api_id: m.api_id },
                raw: true
            }) || { }
        ),
        load(model.Roster, "roster", "match_api_id"),
        load(model.Participant, "participant", "roster_api_id", async (p) =>
            await model.ParticipantStats.findOne({
                where: { participant_api_id: p.api_id },
                raw: true
            })
        ),
        load(model.ParticipantPhases, "participant_phases", "participant_api_id"),
        load(model.Player, "player"),
        load(model.PlayerPoint, "player_point", "player_api_id")
    ]);
})();
