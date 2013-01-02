package net.digitaltsunami.tmeter.mongo.util;

import java.net.UnknownHostException;

import net.digitaltsunami.tmeter.TmeterException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.WriteConcern;

public class MongoDbInstance {

    private final Mongo mongo ;
    private final DB db;
    private final DBCollection collection;
    private final String collectionName;
    
    /**
     * Create an instance with a fully filled out Mongo URI that includes all host, db, and collection information. 
     * @throws TmeterException 
     * @throws UnknownHostException 
     * 
     */
    public MongoDbInstance(MongoURI uri, WriteConcern writeConcern) throws TmeterException, UnknownHostException {
        this(new Mongo(uri), uri.getDatabase(), uri.getCollection(), writeConcern);
    }
    /**
     * @throws TmeterException 
     * @throws UnknownHostException 
     * 
     */
    public MongoDbInstance(String databaseName, String collectionName, WriteConcern writeConcern) throws TmeterException, UnknownHostException {
        this(new Mongo(), databaseName, collectionName, writeConcern);
    }

    /**
     * @throws TmeterException 
     * 
     */
    public MongoDbInstance(Mongo mongo, String databaseName, String collectionName, WriteConcern writeConcern) throws TmeterException {
        super();
        this.mongo = mongo;
        this.db = mongo.getDB(databaseName); 
        this.collectionName = collectionName;
        this.collection = initCollection();
        if (writeConcern != null) {
	        this.collection.setWriteConcern(writeConcern);
        }
    }

    /**
     * @param collectionName
     */
    private DBCollection initCollection() {
        if (db.collectionExists(collectionName)) {
            return  db.getCollection(collectionName);
        }
        else {
            return db.createCollection(collectionName,null);
        }
    }

    /**
     * @return the mongo
     */
    public Mongo getMongo() {
        return mongo;
    }

    /**
     * @return the db
     */
    public DB getDb() {
        return db;
    }

    /**
     * @return the collection
     */
    public DBCollection getCollection() {
        return collection;
    }

    /**
     * @return the collectionName
     */
    public String getCollectionName() {
        return collectionName;
    }
}