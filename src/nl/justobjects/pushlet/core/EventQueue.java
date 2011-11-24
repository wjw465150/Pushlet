// Copyright (c) 2000 Just Objects B.V. <just@justobjects.nl>
// Distributable under LGPL license. See terms of license at gnu.org.

package nl.justobjects.pushlet.core;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import nl.justobjects.pushlet.mongodb.MongodbManager;

/**
 * FIFO queue with guarded suspension. <b>Purpose</b><br>
 * <p/>
 * <b>Implementation</b><br>
 * FIFO queue class implemented with circular array. The enQueue() and deQueue()
 * methods use guarded suspension according to a readers/writers pattern,
 * implemented with java.lang.Object.wait()/notify().
 * <p/>
 * <b>Examples</b><br>
 * <p/>
 * <br>
 * 
 * @author Just van den Broecke - Just Objects &copy;
 * @version $Id: EventQueue.java,v 1.3 2007/11/23 14:33:07 justb Exp $
 */
public class EventQueue { //@wjw_node 属于 Subscriber 的事件队列
  static MongodbManager mongo = MongodbManager.getInstance();
  private static final DBCollection _coll = mongo._db.getCollection("eventqueue");
  static {
    _coll.ensureIndex((DBObject) JSON.parse("{'sessionId': 1}"), (DBObject) JSON.parse("{ns: 'pushlet.eventqueue', name: 'eventqueue_sessionId', unique: false}"));
  }
  private static final int SLEEP_TIME = 200;
  private DBObject findPK;

  /**
   * Defines maximum queue size
   */
  private int capacity = 256;

  private String sessionId;

  /**
   * Construct queue with default (8) capacity.
   */
  public EventQueue(String aSessionId) {
    this(aSessionId, 256);
  }

  /**
   * Construct queue with specified capacity.
   */
  public EventQueue(String aSessionId, int capacity) {
    this.capacity = capacity;

    sessionId = aSessionId;
    findPK = (DBObject) JSON.parse("{'sessionId': '" + sessionId + "'}");
  }

  /**
   * Put item in queue; waits() indefinitely if queue is full.
   */
  public boolean enQueue(Event item) throws InterruptedException {
    return enQueue(item, -1);
  }

  /**
   * Put item in queue; if full wait maxtime.
   */
  public boolean enQueue(Event item, long maxWaitTime) throws InterruptedException {
    if (maxWaitTime < 0) {
      while (isFull()) {
        Thread.sleep(SLEEP_TIME);
      }
    } else {
      long remainTime = maxWaitTime;
      while (isFull() && remainTime >= 0) {
        Thread.sleep(SLEEP_TIME);
        remainTime = remainTime - SLEEP_TIME;
      }

      if (isFull()) {
        return false;
      }
    }

    // Put item in queue
    BasicDBObject obj = new BasicDBObject();
    obj.put("sessionId", sessionId);
    obj.putAll(item.attributes);
    _coll.insert(obj);

    return true;
  }

  /**
   * Get head; if empty wait until something in queue.
   */
  public Event deQueue() throws InterruptedException {
    return deQueue(-1);
  }

  /**
   * Get head; if empty wait for specified time at max.
   */
  public Event deQueue(long maxWaitTime) throws InterruptedException {
    if (maxWaitTime < 0) {
      while (isEmpty()) {
        Thread.sleep(SLEEP_TIME);
      }
    } else {
      long remainTime = maxWaitTime;
      while (isEmpty() && remainTime >= 0) {
        Thread.sleep(SLEEP_TIME);
        remainTime = remainTime - SLEEP_TIME;
      }

      if (isEmpty()) {
        return null;
      }
    }

    // Dequeue item
    Event result = fetchNext();

    // Return dequeued item
    return result;
  }

  /**
   * Get all queued Events.
   */
  public Event[] deQueueAll(long maxWaitTime) throws InterruptedException {
    if (maxWaitTime < 0) {
      while (isEmpty()) {
        Thread.sleep(SLEEP_TIME);
      }
    } else {
      long remainTime = maxWaitTime;
      while (isEmpty() && remainTime >= 0) {
        Thread.sleep(SLEEP_TIME);
        remainTime = remainTime - SLEEP_TIME;
      }

      if (isEmpty()) {
        return null;
      }
    }

    // Dequeue all items item
    java.util.List<Event> listEvent = new java.util.ArrayList<Event>(this.getSize());
    DBObject dbObj;
    Event oneEvent;
    while ((dbObj = _coll.findAndModify(findPK, SessionManager.dbObj_ignore_id, null, true, null, false, false)) != null) {
      oneEvent = new Event(dbObj.toMap());
      listEvent.add(oneEvent);
    }
    Event[] events = listEvent.toArray(new Event[0]);

    // Return dequeued item
    return events;
  }

  public int getSize() {
    return (int) _coll.count(findPK);
  }

  /**
   * Is the queue empty ?
   */
  public boolean isEmpty() {
    return this.getSize() == 0;
  }

  /**
   * Is the queue full ?
   */
  public boolean isFull() {
    return this.getSize() >= capacity;
  }

  /**
   * Circular counter.
   */
  private Event fetchNext() {
    DBObject dbObj = _coll.findAndModify(findPK, SessionManager.dbObj_ignore_id, null, true, null, false, false);
    Event oneEvent = new Event(dbObj.toMap());
    return oneEvent;
  }

  //@wjw_add 清除保存在mongodb里的事件
  public void clear() {
    _coll.remove(findPK);
  }

}
