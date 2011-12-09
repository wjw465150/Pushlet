// Copyright (c) 2000 Just Objects B.V. <just@justobjects.nl>
// Distributable under LGPL license. See terms of license at gnu.org.

package nl.justobjects.pushlet.core;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import nl.justobjects.pushlet.util.Sys;

/**
 * Represents the event data.
 * 
 * @author Just van den Broecke - Just Objects &copy;
 * @version $Id: Event.java,v 1.13 2007/11/23 14:33:07 justb Exp $
 */
public class Event implements Protocol, Serializable {
  protected Map attributes = new HashMap(3);

  public Event(String anEventType) {
    this(anEventType, null);
  }

  public Event(String anEventType, Map theAttributes) {

    if (theAttributes != null) {
      setAttrs(theAttributes);
    }

    // Set required field event type
    setField(P_EVENT, anEventType);

    // Set time in seconds since 1970
    setField(P_TIME, System.currentTimeMillis() / 1000);
  }

  public Event(Map theAttributes) {
    if (!theAttributes.containsKey(P_EVENT)) {
      throw new IllegalArgumentException(P_EVENT + " not found in attributes");
    }
    setAttrs(theAttributes);
  }

  public static Event createDataEvent(String aSubject) {
    return createDataEvent(aSubject, null);
  }

  public static Event createDataEvent(String aSubject, Map theAttributes) {
    Event dataEvent = new Event(E_DATA, theAttributes);
    dataEvent.setField(P_SUBJECT, aSubject);
    return dataEvent;
  }

  public String getEventType() {
    return getField(P_EVENT);
  }

  public String getSubject() {
    return getField(P_SUBJECT);
  }

  public void setField(String name, String value) {
    attributes.put(name, value);
  }

  public void setField(String name, int value) {
    attributes.put(name, value + "");
  }

  public void setField(String name, long value) {
    attributes.put(name, value + "");
  }

  public String getField(String name) {
    return (String) attributes.get(name);
  }

  /**
   * Return field; if null return default.
   */
  public String getField(String name, String aDefault) {
    String result = getField(name);
    return result == null ? aDefault : result;
  }

  public Iterator getFieldNames() {
    return attributes.keySet().iterator();
  }

  public String toString() {
    return attributes.toString();
  }

  /**
   * Convert to HTTP query string.
   */
  public String toQueryString() {
    String queryString = "";
    String amp = "";
    for (Iterator iter = getFieldNames(); iter.hasNext();) {
      String nextAttrName = (String) iter.next();
      String nextAttrValue = getField(nextAttrName);

      //@wjw_add 为了正确编码,必须使用URLEncoder.encode(url,"UTF-8")
      try {
        nextAttrName = java.net.URLEncoder.encode(nextAttrName, "UTF-8");
      } catch (UnsupportedEncodingException e) {
      }
      try {
        nextAttrValue = java.net.URLEncoder.encode(nextAttrValue, "UTF-8");
      } catch (UnsupportedEncodingException e) {
      }

      queryString = queryString + amp + nextAttrName + "=" + nextAttrValue;
      // After first add "&".
      amp = "&";
    }

    return queryString;
  }

  public String toXML(boolean strict) {
    String xmlString = "<event ";
    for (Iterator iter = getFieldNames(); iter.hasNext();) {
      String nextAttrName = (String) iter.next();
      String nextAttrValue = getField(nextAttrName);
      xmlString = xmlString + nextAttrName + "=\"" + (strict ? Sys.forHTMLTag(nextAttrValue) : nextAttrValue) + "\" ";
    }

    xmlString += "/>";
    return xmlString;
  }

  public String toXML() {
    return toXML(false);
  }

  @SuppressWarnings("unchecked")
  public String toJson() {
    StringBuilder jsonString = new StringBuilder("{ ");
    String nextAttrName;
    String nextAttrValue;
    boolean firstLoop = true;
    for (Iterator<String> iter = getFieldNames(); iter.hasNext();) {
      nextAttrName = iter.next();
      nextAttrValue = getField(nextAttrName);
      if (firstLoop) {
        firstLoop = false;
      } else {
        jsonString.append(",");
      }
      jsonString.append(Sys.quote(nextAttrName) + ": " + Sys.quote(nextAttrValue) + " ");
    }

    jsonString.append(" }");
    return jsonString.toString();
  }

  public Object clone() {
    // Clone the Event by using copy constructor
    return new Event(attributes);
  }

  /**
   * Copy given attributes into event attributes
   */
  private void setAttrs(Map theAttributes) {
    attributes.putAll(theAttributes);
  }

}
