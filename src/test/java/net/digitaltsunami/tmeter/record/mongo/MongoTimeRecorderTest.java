package net.digitaltsunami.tmeter.record.mongo;

import net.digitaltsunami.tmeter.Timer;
import net.digitaltsunami.tmeter.TmeterException;

import org.junit.Before;
import org.junit.Test;

public class MongoTimeRecorderTest {

    private MongoTimeRecorder recorder;
    @Before
    public void setup() throws TmeterException {
        recorder = new MongoTimeRecorder();
    }
    @Test
    public void testTimerWithNoNotes() {
        Timer t = new Timer("UnitTest");
        t.setTimeRecorder(recorder);
        t.start();
        t.stop();
    }
    @Test
    public void testTimerWithNonKeyedNotes() {
        Timer t = new Timer("UnitTest");
        t.setTimeRecorder(recorder);
        t.start();
        t.stop(false, "noteVal1", "noteVal2");
    }
    @Test
    public void testTimerWithKeyedNotes() {
        Timer t = new Timer("UnitTest");
        t.setTimeRecorder(recorder);
        t.start();
        t.stop(true, "noteKey1", "noteVal1");
    }
    @Test
    public void testTimerWithKeyedNotesMixedValues() {
        Timer t = new Timer("UnitTest");
        t.setTimeRecorder(recorder);
        t.start();
        t.stop(true, "noteKey1", 1, "noteKey2", 3.7, "noteKey3", "noteVal3");
    }

}
