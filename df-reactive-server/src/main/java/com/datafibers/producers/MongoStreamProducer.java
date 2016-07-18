package com.datafibers.producers;

import com.datafibers.util.ServerFunc;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

/**
 * This class is used to write metadata to mongodb so that we have full history of meta data.
 */
public class MongoStreamProducer {

    MongoClient client;

    public MongoStreamProducer (Vertx vertx, JsonObject config) {
        client = MongoClient.createShared(vertx, config);
    }

    public MongoStreamProducer (Vertx vertx) {
        client = MongoClient.createShared(vertx, new JsonObject());
    }

    public void sendMessages(String dbName, String message) {
        JsonObject document = new JsonObject(message);

        client.insert(dbName, document, res -> {

            if (res.succeeded()) {

                String id = res.result();
                ServerFunc.printToConsole("INFO","Sending Metadata to Mongodb Successfully @" + id);

            } else {
                res.cause().printStackTrace();
                ServerFunc.printToConsole("INFO","Sending Metadata to Mongodb Failed!!!");
            }

        });


    }
}
