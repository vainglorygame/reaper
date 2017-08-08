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
                return await model.ParticipantPhases.findAll({
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
                            model.Roster,
                            model.Match
                        ]
                    } ],
                    raw: true
                })
            } ) );
        db_profiler.done("database transaction");

        const es_profiler = logger.startTimer();
        await elastic.bulk({
            body: [].concat(... data.map((d) => [
                { index: {
                    _index: "phase",
                    _type: "phase",
                    _id: d.id
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
