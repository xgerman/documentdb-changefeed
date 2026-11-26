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
    const [first, second] = await Promise.all([
        collection.watch([], { batchSize: 1, maxAwaitTimeMS: 50 }),
        collection.watch([], { batchSize: 1, maxAwaitTimeMS: 50 })
    ]);
    check.deepEqual(await Promise.all([first.tryNext(), second.tryNext()]), [null, null]);
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

    const updates = db.getSiblingDB("cf_watch_updates").events;
    await updates.insertOne({ _id: "tracked", counter: 0, remove: true, items: [1, 2] });
    const updateStream = await updates.watch([], { batchSize: 1, maxAwaitTimeMS: 50 });
    check.equal(await updateStream.tryNext(), null);
    await updates.updateOne(
        { _id: "tracked" },
        { $inc: { counter: 2 }, $set: { "nested.value": 3 }, $unset: { remove: "" } }
    );
    const changed = await nextEvent(updateStream);
    check.equal(changed.operationType, "update");
    check.equal(changed.documentKey._id, "tracked");
    check.equal(changed.updateDescription.updatedFields.counter, 2);
    check.equal(changed.updateDescription.updatedFields["nested.value"], 3);
    check.equal(EJSON.stringify(changed.updateDescription.removedFields), '["remove"]');
    check.equal(changed.fullDocument, undefined);
    await updates.replaceOne({ _id: "tracked" }, { _id: "tracked", payload: "replacement" });
    const replaced = await nextEvent(updateStream);
    check.equal(replaced.operationType, "replace");
    check.equal(replaced.fullDocument.payload, "replacement");
    check.equal(replaced.updateDescription, undefined);
    const updateResume = await updates.watch(
        [], { resumeAfter: changed._id, batchSize: 1, maxAwaitTimeMS: 50 }
    );
    check.equal(EJSON.stringify(await nextEvent(updateResume)), EJSON.stringify(replaced));
    await updates.updateOne({ _id: "tracked" }, [{ $set: { pipeline: true } }]);
    const pipeline = await nextEvent(updateStream);
    check.equal(pipeline.operationType, "replace");
    check.equal(pipeline.fullDocument.pipeline, true);
    await updates.updateMany({ _id: "tracked" }, { $set: { pipeline: true } });
    check.equal(await updateStream.tryNext(), null);
    await updates.findOneAndUpdate({ _id: "tracked" }, { $set: { modified: true } });
    const modified = await nextEvent(updateStream);
    check.equal(modified.operationType, "update");
    check.equal(modified.updateDescription.updatedFields.modified, true);
    await updateStream.close();
    await updateResume.close();
    await first.close();
    await second.close();
    await resumed.close();
    await filtered.close();
    print("PASS client watch/getMore: inserts, deletes, updates, replacements/pipelines, no-ops, independent readers, exclusive resume and filtering");
}

main();
