#!/usr/bin/node
/* jshint esnext:true */
"use strict";

const amqp = require("amqplib"),
    Promise = require("bluebird"),
    winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    Seq = require("sequelize"),
    elasticsearch = require("elasticsearch");

const RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    DATABASE_URI = process.env.DATABASE_URI,
    ELASTICSEARCH_URI = process.env.ELASTICSEARCH_URI || "localhost:9200",
    QUEUE = process.env.QUEUE || "reap",
    INDEX = process.env.INDEX || "phase",
    JOIN_PLAYER = process.env.JOIN_PLAYER == "true",  // include player records
    SPLIT_INDEX = process.env.SPLIT_INDEX != "false",  // split index into index_seriesName
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    BATCHSIZE = parseInt(process.env.BATCHSIZE) || 10,
    IDLE_TIMEOUT = parseInt(process.env.IDLE_TIMEOUT) || 1000,  // ms
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
        tags: ["backend", "reaper", QUEUE],
        json: true
    });


amqp.connect(RABBITMQ_URI).then(async (rabbit) => {
    process.on("SIGINT", () => {
        rabbit.close();
        process.exit();
    });

    // connect to rabbit & db
    const seq = new Seq(DATABASE_URI, {
            logging: false,
            pool: {
                max: MAXCONNS
            }
        }),
        elastic = new elasticsearch.Client({ host: ELASTICSEARCH_URI, log: "info" });

    const ch = await rabbit.createChannel();
    await ch.assertQueue(QUEUE, { durable: true });
    await ch.assertQueue(QUEUE + "_failed", { durable: true });
    await ch.prefetch(BATCHSIZE);

    logger.info("configuration", {
        QUEUE, BATCHSIZE, MAXCONNS, IDLE_TIMEOUT
    });

    const model = require("../orm/model")(seq, Seq);

    let phase_data = new Set();
    let msg_buffer = new Set();
    let idle_timer = undefined;

    ch.consume(QUEUE, async (msg) => {
        const payload = JSON.parse(msg.content);
        phase_data.add(payload);
        msg_buffer.add(msg);

        // timeout after last job
        if (idle_timer != undefined)
            clearTimeout(idle_timer);
        idle_timer = setTimeout(tryProcess, IDLE_TIMEOUT);
        if (phase_data.size == BATCHSIZE)
            await tryProcess();
    }, { noAck: false });

    // wrap process() in message handler
    async function tryProcess() {
        const msgs = new Set(msg_buffer);
        msg_buffer.clear();

        logger.info("processing batch");

        // clean up to allow reaper to accept while we wait for db
        clearTimeout(idle_timer);
        idle_timer = undefined;

        const phase_objects = new Set(phase_data);
        phase_data.clear();

        try {
            await reap(phase_objects);

            logger.info("acking batch", { size: msgs.size });
            await Promise.map(msgs, async (m) => await ch.ack(m));
        } catch (err) {
            // log, move to error queue and NACK
            logger.error(err);
            await Promise.map(msgs, async (m) => {
                await ch.sendToQueue(QUEUE + "_failed",
                    m.content, { persistent: true });
                await ch.nack(m, false, false);
            });
        }
    }

    async function reap(phase_objects) {
        const db_profiler = logger.startTimer(),
            data = [].concat(... await Promise.map(phase_objects, async (po) => {
                // TODO would be better to get all in bulk using the id
                let phases = await model.ParticipantPhases.findAll({
                    attributes: [
                        "id", "created_at", "updated_at", "start", "end",
                        "participant_api_id",
                        "kills", "deaths", "assists", /* "farm" ,*/
                        "minion_kills", "jungle_kills",
                        "non_jungle_minion_kills",
                        "crystal_mine_captures", "gold_mine_captures",
                        "kraken_captures", "turret_captures",
                        /*"gold",*/
                        "dmg_true_hero", "dmg_true_kraken",
                        "dmg_true_turret", "dmg_true_vain_turret",
                        "dmg_true_others",
                        "dmg_dealt_hero", "dmg_dealt_kraken",
                        "dmg_dealt_turret", "dmg_dealt_vain_turret",
                        "dmg_dealt_others",
                        "dmg_rcvd_dealt_hero", "dmg_rcvd_true_hero",
                        "dmg_rcvd_dealt_others", "dmg_rcvd_true_others",
                        "ability_a_level", "ability_b_level", "ability_c_level",
                        "hero_level",
                        /* scores, */
                        "draft_position", "ban", "pick",
                        /*seq.fn("COLUMN_JSON", "items"),*/
                        [ seq.cast(seq.fn("COLUMN_JSON", seq.col("participant_phases.item_grants")), "char"), "item_grants" ],
                        [ seq.cast(seq.fn("COLUMN_JSON", seq.col("participant_phases.item_sells")), "char"), "item_sells" ],
                        "ability_a_use", "ability_b_use", "ability_c_use",
                        "ability_a_damage_true", "ability_a_damage_dealt",
                        "ability_b_damage_true", "ability_b_damage_dealt",
                        "ability_c_damage_true", "ability_c_damage_dealt",
                        "ability_perk_damage_true", "ability_perk_damage_dealt",
                        "ability_aa_damage_true", "ability_aa_damage_dealt",
                        "ability_aacrit_damage_true", "ability_aacrit_damage_dealt",
                        [ seq.cast(seq.fn("COLUMN_JSON", seq.col("participant_phases.item_uses")), "char"), "item_uses" ]/*,
                        seq.fn("COLUMN_JSON", "player_damage")*/
                    ],
                    where: {
                        "$participant.match_api_id$": po.match_api_id,
                        start: po.start,
                        end: po.end
                    },
                    include: [ {
                        model: model.Participant,
                        include: [
                            model.Region,
                            model.Hero,
                            model.Series,
                            model.GameMode,
                            model.Role,

                            model.ParticipantStats,
                            {
                                model: model.ParticipantItems,
                                attributes: [
                                    "id", "shard_id", "participant_api_id",
                                    [ seq.cast(seq.fn("COLUMN_JSON", seq.col("participant->participant_items.items")), "char"), "items" ],
                                    [ seq.cast(seq.fn("COLUMN_JSON", seq.col("participant->participant_items.item_grants")), "char"), "item_grants" ],
                                    [ seq.cast(seq.fn("COLUMN_JSON", seq.col("participant->participant_items.item_uses")), "char"), "item_uses" ],
                                    [ seq.cast(seq.fn("COLUMN_JSON", seq.col("participant->participant_items.item_sells")), "char"), "item_sells" ],
                                    "surrender",
                                    "trueskill_ranked_mu", "trueskill_ranked_sigma"
                                ],
                            },
                            model.Roster,
                            model.Match
                        ].concat(JOIN_PLAYER? [ model.Player ] : [])
                    }, {
                        model: model.Hero,
                        as: "hero_ban"
                    }, {
                        model: model.Hero,
                        as: "hero_pick"
                    }
                    ],
                    raw: true
                });
                // parse dynamic columns output
                phases.forEach((data) => {
                    //data.items = JSON.parse(data.items);
                    if (data.item_grants != "")
                        data.item_grants = JSON.parse(data.item_grants);
                    else data.item_grants = {};
                    if (data.item_sells != "")
                        data.item_sells = JSON.parse(data.item_sells);
                    else data.item_sells = {};
                    if (data.item_uses != "")
                        data.item_uses = JSON.parse(data.item_uses);
                    else data.item_uses = {};

                    if (data["participant.participant_items.items"] != "")
                        data["participant.participant_items.items"] = JSON.parse(data["participant.participant_items.items"]);
                    else data["participant.participant_items.items"] = {};
                    if (data["participant.participant_items.item_grants"] != "")
                        data["participant.participant_items.item_grants"] = JSON.parse(data["participant.participant_items.item_grants"]);
                    else data["participant.participant_items.item_grants"] = {};
                    if (data["participant.participant_items.item_sells"] != "")
                        data["participant.participant_items.item_sells"] = JSON.parse(data["participant.participant_items.item_sells"]);
                    else data["participant.participant_items.item_sells"] = {};
                    if (data["participant.participant_items.item_uses"] != "")
                        data["participant.participant_items.item_uses"] = JSON.parse(data["participant.participant_items.item_uses"]);
                    else data["participant.participant_items.item_uses"] = {};
                });
                return phases;
            } ) );
        db_profiler.done("database transaction");

        if (data.length == 0) {
            logger.error("no data!");
            return;
        }

        const es_profiler = logger.startTimer();
        await elastic.bulk({
            body: [].concat(... data.map((d) => [
                { index: {
                    _index:
                        SPLIT_INDEX?
                            `${INDEX}_${d["participant.series.name"].toLowerCase().replace(/ |\./g, "_")}`
                        :
                            INDEX,
                    _type: INDEX,
                    _id: `${d.participant_api_id}@${d.start}+${d.end}`
                } },
                d
            ]) )
        });
        es_profiler.done("elastic bulk request");
    }
});

process.on("unhandledRejection", (err) => {
    logger.error(err);
    process.exit(1);  // fail hard and die
});
