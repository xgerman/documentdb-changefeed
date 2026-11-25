/* Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License
 */
const check = require("assert").strict;

async function nextEvent(stream) {
    for (let attempt = 0; attempt < 30; attempt++) {
        const event = await stream.tryNext();
        if (event !== null) {
            return event;
        }
        await sleep(100);
    }
    throw new Error("Timed out waiting for a changefeed event");
}

async function main() {
    const admin = db.getSiblingDB("admin");
    const authenticated = await admin.auth("cf_client", process.env.CF_TEST_PASSWORD);
    check.equal(authenticated.ok, 1);
    const collection = db.getSiblingDB("cf_watch").events;
    const first = await collection.watch([], { batchSize: 1, maxAwaitTimeMS: 50 });
    const second = await collection.watch([], { batchSize: 1, maxAwaitTimeMS: 50 });
    check.equal(await first.tryNext(), null);
    check.equal(await second.tryNext(), null);
    await collection.insertMany([{ _id: "one", value: 1 }, { _id: "two", value: 2 }]);
    const one = await nextEvent(first);
    check.equal(one.operationType, "insert");
    check.equal(one.documentKey._id, "one");
    check.equal(one.fullDocument.value, 1);
    check.equal(one.ns.db, "cf_watch");
    check.equal(one.ns.coll, "events");
    const two = await nextEvent(first);
    check.equal(two.documentKey._id, "two");
    check.equal(EJSON.stringify(await nextEvent(second)), EJSON.stringify(one));
    check.equal(EJSON.stringify(await nextEvent(second)), EJSON.stringify(two));

    const resumed = await collection.watch([], { resumeAfter: one._id, batchSize: 1, maxAwaitTimeMS: 50 });
    check.equal(EJSON.stringify(await nextEvent(resumed)), EJSON.stringify(two));
    await collection.deleteOne({ _id: "one" });
    const deleted = await nextEvent(first);
    check.equal(deleted.operationType, "delete");
    check.equal(deleted.documentKey._id, "one");
    check.equal(deleted.fullDocument, undefined);

    const filtered = await collection.watch(
        [{ $match: { operationType: "delete" } }],
        { batchSize: 1, maxAwaitTimeMS: 50 }
    );
    check.equal(await filtered.tryNext(), null);
    await collection.insertOne({ _id: "filtered" });
    check.equal(await filtered.tryNext(), null);
    await collection.deleteOne({ _id: "filtered" });
    check.equal((await nextEvent(filtered)).documentKey._id, "filtered");
    await first.close();
    await second.close();
    await resumed.close();
    await filtered.close();
    print("PASS client watch/getMore: inserts, deletes, independent readers, exclusive resume and filtering");
}

main();
