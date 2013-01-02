package net.digitaltsunami.tmeter.mongo.util;

import java.net.UnknownHostException;

import net.digitaltsunami.tmeter.TmeterException;

import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.WriteConcern;

public class MongoDbInstanceBuilder {
    public static MongoDbInstanceBuilder init() {
        return new MongoDbInstanceBuilder();
    }

    private MongoURI uri;
    private Mongo mongo;
    private String host;
    private String database;
    private String collection;
    private WriteConcern writeConcern;

    public MongoDbInstanceBuilder setUri(String uri) {
        this.uri = new MongoURI(uri);
        return this;
    }

    public MongoDbInstanceBuilder setMongo(Mongo mongo) {
        this.mongo = mongo;
        return this;
    }

    public MongoDbInstanceBuilder setHost(String host) {
        this.host = host;
        return this;
    }

    public MongoDbInstanceBuilder setDatabase(String database) {
        this.database = database;
        return this;
    }

    public MongoDbInstanceBuilder setCollection(String collection) {
        this.collection = collection;
        return this;
    }

    public MongoDbInstanceBuilder setWriteConcern(WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
        return this;
    }

    public MongoDbInstance build() throws TmeterException {
        if (writeConcern == null) {
            writeConcern = WriteConcern.NORMAL;
        }
        MongoDbInstance instance = null;
        if (uri != null) {
            try {
                collection = (hasText(collection)) ? collection : uri.getCollection();
		        if (!hasText(collection)) {
		            throw new IllegalStateException(
		                    "Collection must be provided.  Cannot create Mongo DB Instance");
		        }
                database = (hasText(database)) ? database : uri.getDatabase();
                mongo = new Mongo(uri);
                instance = new MongoDbInstance(mongo, database, collection, writeConcern);
            } catch (UnknownHostException e) {
                throw new TmeterException("Failed to create Mongo instance. ", e);
            }
        }
        else {
            // host and db come from parameters
	        if (!hasText(collection)) {
	            throw new IllegalStateException(
	                    "Collection must be provided.  Cannot create Mongo DB Instance");
	        }
            try {
                instance = new MongoDbInstance(database, collection, writeConcern);
            } catch (UnknownHostException e) {
                throw new TmeterException("Failed to create Mongo instance. ", e);
            }
        }
        return instance;
    }

    /**
     * Returns true if the string object is not null and has a length greater
     * than zero. Whitespace only fields will be considered as having text.
     * 
     * @param string
     * @return
     */
    public static boolean hasText(String string) {
        return string != null && string.length() > 0;
    }
}
