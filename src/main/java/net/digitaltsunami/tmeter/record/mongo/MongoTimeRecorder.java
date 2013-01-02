/**
 * 
 */
package net.digitaltsunami.tmeter.record.mongo;

import java.util.Date;

import net.digitaltsunami.tmeter.Timer;
import net.digitaltsunami.tmeter.TimerNotes;
import net.digitaltsunami.tmeter.TmeterException;
import net.digitaltsunami.tmeter.action.TimeRecorderAction;
import net.digitaltsunami.tmeter.mongo.util.MongoDbInstance;
import net.digitaltsunami.tmeter.mongo.util.MongoDbInstanceBuilder;
import net.digitaltsunami.tmeter.record.TimeRecorder;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

/**
 * Time recorder using mongodb as persistent store.
 * <p>
 * <strong>Performance Note</strong>
 * <p>
 * Note that the default Write Concern is to w=0 {@link WriteConcern#NONE} or
 * {@link WriteConcern#UNACKNOWLEDGED} depending on the driver version. In this
 * mode, time recordings may be lost, but the recording should be fast. If
 * changing to a more robust write concern, be aware that this may noticeably
 * slow down the timed process. For timing of quick or high volume tasks, the
 * write concern should be left at the default or the persistence of time
 * recording completed off of the timed thread (see {@link TimeRecorderAction}).
 * 
 * @author dhagberg
 * 
 */
public class MongoTimeRecorder implements TimeRecorder {
    /**
     * Default MongoDB database name.
     */
    public static final String DEFAULT_DB_NAME = "tMeter";
    /**
     * Default MongoDB collection name.
     */
    public static final String DEFAULT_COLLECTION_NAME = "taskTimings";
    /**
     * The default write concern for the collection is w=0. This correlates to
     * {@link WriteConcern#UNACKNOWLEDGED} or {@link WriteConcern#NONE}
     * depending on your driver version. In either case, only network errors
     * will be detected.
     */
    protected static final WriteConcern DEFAULT_WRITE_CONCERN = new WriteConcern(-1);
    // TODO: put this back to 0

    private MongoDbInstance mongodb;

    /**
     * Create a {@link TimeRecorder} that will write all timers to the default
     * database, collection, and write concern.
     * 
     * @throws TmeterException
     * @see {@link #DEFAULT_DB_NAME}
     * @see {@link #DEFAULT_COLLECTION_NAME}
     * @see {@link #DEFAULT_WRITE_CONCERN}
     */
    public MongoTimeRecorder() throws TmeterException {
        this(DEFAULT_DB_NAME, DEFAULT_COLLECTION_NAME, DEFAULT_WRITE_CONCERN);
    }

    /**
     * Create a {@link TimeRecorder} that will write all timers to the
     * db.collection provided.
     * <p>
     * Allows setting of the {@link WriteConcern}. See note below on this
     * setting.
     * 
     * @param db
     *            Name of database under which the collection exists.
     * @param collectionName
     *            Name of collection to which the timers will be written.
     * @param writeConcern
     *            write concern specifying the level of acknowledgment of the
     *            write operation. See class note regarding write concerns.
     * @throws TmeterException
     */
    public MongoTimeRecorder(String databaseName, String collectionName, WriteConcern writeConcern)
            throws TmeterException {
        this(MongoDbInstanceBuilder.init().setCollection(collectionName)
                .setDatabase(databaseName).setWriteConcern(writeConcern).build());
    }

    /**
     * Create a {@link TimeRecorder} that will write all timers to the
     * db.collection provided via the MongoDbInstance. For other combinations
     * that those provided here, a {@link MongoDbInstance} can be built using
     * the constructors or {@link MongoDbInstanceBuilder}.
     * <p>
     * Setting of the {@link WriteConcern} will have been already configured.
     * See class note regarding this setting.
     * 
     * @param mongodb
     *            Fully configured {@link MongoDbInstance}.
     * @throws TmeterException
     */
    public MongoTimeRecorder(MongoDbInstance mongodb)
            throws TmeterException {
        this.mongodb = mongodb;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * net.digitaltsunami.tmeter.record.TimeRecorder#record(net.digitaltsunami.tmeter.Timer)
     */
    public void record(Timer timer) {
        DBObject dbobj = new BasicDBObject();
        dbobj.put("task", timer.getTaskName());
        dbobj.put("startTime", new Date(timer.getStartTimeMillis()));
        dbobj.put("millis", timer.getElapsedMillis());
        dbobj.put("nanos", timer.getElapsedNanos());
        dbobj.put("current", timer.getConcurrent());

        TimerNotes notes = timer.getNotes();
        if (notes != null) {
            if (notes.isKeyed()) {
                for (String key : notes.getKeys()) {
                    dbobj.put(key, notes.getValue(key));
                }
            }
            else {
                BasicDBList notesList = new BasicDBList();
                for (Object value : notes.getNotes()) {
                    notesList.add(value);
                }
                dbobj.put("notes", notesList);

            }
        }

        mongodb.getCollection().insert(dbobj);
    }

}
