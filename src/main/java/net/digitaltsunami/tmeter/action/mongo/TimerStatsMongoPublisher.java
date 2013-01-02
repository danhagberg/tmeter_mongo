/**
 * 
 */
package net.digitaltsunami.tmeter.action.mongo;

import java.util.Collection;
import java.util.Date;

import net.digitaltsunami.tmeter.TimerBasicStatistics;
import net.digitaltsunami.tmeter.TmeterException;
import net.digitaltsunami.tmeter.action.TimerStatsPublisher;
import net.digitaltsunami.tmeter.mongo.util.MongoDbInstance;
import net.digitaltsunami.tmeter.mongo.util.MongoDbInstanceBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * @author dhagberg
 * 
 */
public class TimerStatsMongoPublisher implements TimerStatsPublisher {
    private static final String DEFAULT_DB_NAME = "tMeter";
    private static final String DEFAULT_COLLECION_NAME = "tmBasicStats";
    private MongoDbInstance mongodb;

    /**
     * @throws TmeterException
     * 
     */
    public TimerStatsMongoPublisher() throws TmeterException {
        this(DEFAULT_DB_NAME, DEFAULT_COLLECION_NAME);
    }

    /**
     * @throws TmeterException
     * 
     */
    public TimerStatsMongoPublisher(String databaseName, String collectionName)
            throws TmeterException {
        this(MongoDbInstanceBuilder.init().setDatabase(databaseName).setCollection(collectionName).build());
    }

    /**
     * @throws TmeterException
     * 
     */
    public TimerStatsMongoPublisher(MongoDbInstance mongodb) 
            throws TmeterException {
        super();
        this.mongodb = mongodb;
    }

    /*
     * (non-Javadoc)
     * 
     * @see net.digitaltsunami.tmeter.action.TimerStatsPublisher#publish(net.
     * digitaltsunami.tmeter.TimerBasicStatistics)
     */
    public void publish(TimerBasicStatistics stats) {
        DBObject dbobj = new BasicDBObject();
        dbobj.put("task", stats.getTaskName());
        dbobj.put("recdate", new Date());
        dbobj.put("minNanos", stats.getMinElapsedNanos());
        dbobj.put("maxNanos", stats.getMaxElapsedNanos());
        dbobj.put("meanNanos", stats.getAverageElapsedNanos());
        dbobj.put("count", stats.getCount());
        dbobj.put("stdDevNanos", stats.getStdDevElapsedNanos());
        dbobj.put("totalNanos", stats.getTotalElapsedNanos());

        mongodb.getCollection().insert(dbobj);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * net.digitaltsunami.tmeter.action.TimerStatsPublisher#reset(java.util.
     * Collection)
     */
    public void reset(Collection<TimerBasicStatistics> stats) {
        stats.clear();
    }
}
