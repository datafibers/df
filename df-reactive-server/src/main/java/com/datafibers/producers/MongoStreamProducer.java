package com.datafibers.producers;

import com.datafibers.conf.ConfigApp;
import com.datafibers.util.ServerFunc;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

/**
 * This class is used to write/stream data, such as metadata, to mongodb.
 * One example of usage is to write metadata to mongodb for now since we do not finalize meta schema yet.
 */
public class MongoStreamProducer {

    MongoClient client;

    public MongoStreamProducer (Vertx vertx, JsonObject config) {
        client = MongoClient.createShared(vertx, config);
    }

    public MongoStreamProducer (Vertx vertx) {
        JsonObject jo = null;
        if(ConfigApp.getMetaMongodbConfig() == null) {
            jo = new JsonObject();
        } else new JsonObject(ConfigApp.getMetaMongodbConfig());
        client = MongoClient.createShared(vertx, jo);
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
