/*
 * Pushlet client using AJAX XMLHttpRequest.
 *
 * DESCRIPTION
 * This file provides self-contained support for using the
 * Pushlet protocol through AJAX-technology. The XMLHttpRequest is used
 * to exchange the Pushlet protocol XML messages (may use JSON in later versions).
 * Currently only HTTP GET is used in asynchronous mode.
 *
 * The Pushlet protocol provides a Publish/Subscribe service for
 * simple messages. The Pushlet server provides session management (join/leave),
 * subscription management (subscribe/unsubscribe), server originated push
 * and publication (publish).
 *
 * For subscriptions server-push is emulated using a single
 * long-lived XMLHttpRequests (Pushlet pull mode) where the server holds the
 * request until events arrive for which a session has subscriptions.
 * This is thus different from polling. In future versions XML streaming
 * may be used since this is currently only supported in Moz-family browsers.
 *
 * Users should supply global callback functions for the events they are interested in.
 * For now see _onEvent() for the specific functions that are called.
 * The most important one is onData(). If onEvent() is available that catches
 * all events. All callback functions have a single
 * argument with a PushletEvent object.
 * A future version should provide a more OO (Observer) approach.
 *
 * EXAMPLES
 * PL.join();
 * PL.listen();
 * PL.subscribe('/temperature');
 * // or shorter
 * PL.joinListen('/temperature');
 * // You provide as callback:
 * onData(pushletEvent);
 * See examples in the Pushlet distribution (e.g. webapps/pushlet/examples/ajax)
 *
 * WHY
 * IMO using XMLHttpRequest has many advantages over the original JS streaming:
 * more stability, no browser busy-bees, better integration with other AJAX frameworks,
 * more debugable, more understandable, ...
 *
 * $Id: ajax-pushlet-client.js,v 1.7 2007/07/27 11:45:08 justb Exp $
 */

/** Namespaced Pushlet functions. */
var PL = {
	NV_P_FORMAT: 'p_format=xml-strict',
	NV_P_MODE: 'p_mode=pull',
	pushletURL: null,
	webRoot: null,
	sessionId: null,
	STATE_ERROR: -2,
	STATE_ABORT: -1,
	STATE_NULL: 1,
	STATE_READY: 2,
	STATE_JOINED: 3,
	STATE_LISTENING: 3,
	state: 1,

/************** START PUBLIC FUNCTIONS  **************/

/** Send heartbeat. */
	heartbeat: function() {
		PL._doRequest('hb');
	},

/** Join. */
	join: function(aSession_id) {
		PL.sessionId = null;
		if(aSession_id) {  //@wjw_add 
      PL.sessionId = aSession_id;
		}

		// Streaming is only supported in Mozilla. E.g. IE does not allow access to responseText on readyState == 3
		PL._doRequest('join', PL.NV_P_FORMAT + '&' + PL.NV_P_MODE);
	},

/** Join, listen and subscribe. */
	joinListen: function(aSubject,aSession_id) {
		PL._setStatus('join-listen ' + aSubject);
		// PL.join();
		// PL.listen(aSubject);

		PL.sessionId = null;
    if(aSession_id) {  //@wjw_add 
      PL.sessionId = aSession_id;
    }

		// Create event URI for listen
		var query = PL.NV_P_FORMAT + '&' + PL.NV_P_MODE;

		// Optional subject to subscribe to
		if (aSubject) {
			query = query + '&p_subject=' + encodeURIComponent(aSubject);
		}

		PL._doRequest('join-listen', query);

	},

/** Close pushlet session. */
	leave: function() {
		PL._doRequest('leave');
	},

/** Listen on event channel. */
	listen: function(aSubject) {

		// Create event URI for listen
		var query = PL.NV_P_MODE;

		// Optional subject to subscribe to
		if (aSubject) {
			query = query + '&p_subject=' + encodeURIComponent(aSubject);
		}

		PL._doRequest('listen', query);
	},

/** Publish to subject. */
	publish: function(aSubject, theQueryArgs) {

		var query = 'p_subject=' + encodeURIComponent(aSubject);
		if (theQueryArgs) {
			query = query + '&' + theQueryArgs;
		}

		PL._doRequest('publish', query);
	},

/** Publish to subject 给在线用户. */
  publish_to_online: function(aSubject, theQueryArgs) {

    var query = 'p_subject=' + encodeURIComponent(aSubject);
    if (theQueryArgs) {
      query = query + '&' + theQueryArgs;
    }

    PL._doRequest('publish_to_online', query);
  },
	
/** Publish Json to subject. */
  publishJson: function(aSubject, jsonMsg) {

    var query = 'p_subject=' + encodeURIComponent(aSubject);
    if (jsonMsg) {
      query = query + '&' + objectToQuery(jsonMsg);
    }

    PL._doRequest('publish', query);
  },

/** Publish Json to subject 给在线用户. */
  publishJson_to_online: function(aSubject, jsonMsg) {

    var query = 'p_subject=' + encodeURIComponent(aSubject);
    if (jsonMsg) {
      query = query + '&' + objectToQuery(jsonMsg);
    }

    PL._doRequest('publish_to_online', query);
  },
  
/** Subscribe to (comma separated) subject(s). */
	subscribe: function(aSubject, aLabel) {

		var query = 'p_subject=' + encodeURIComponent(aSubject);
		if (aLabel) {
			query = query + '&p_label=' + encodeURIComponent(aLabel);
		}
		PL._doRequest('subscribe', query);

	},

/** Unsubscribe from (all) subject(s). */
	unsubscribe: function(aSubscriptionId) {
		var query;

		// If no sid we unsubscribe from all subscriptions
		if (aSubscriptionId) {
			query = 'p_sid=' + encodeURIComponent(aSubscriptionId);
		}
		PL._doRequest('unsubscribe', query);
	},

	setDebug: function(bool) {
		PL.debugOn = bool;
	},


/************** END PUBLIC FUNCTIONS  **************/

// Cross-browser add event listener to element
	_addEvent: function (elm, evType, callback, useCapture) {
		var obj = PL._getObject(elm);
		if (obj.addEventListener) {
			obj.addEventListener(evType, callback, useCapture);
			return true;
		} else if (obj.attachEvent) {
			var r = obj.attachEvent('on' + evType, callback);
			return r;
		} else {
			obj['on' + evType] = callback;
		}
	},

	_doCallback: function(event, cbFunction) {
		// Do specific callback function if provided by client
		if (cbFunction) {
			// Do specific callback like onData(), onJoinAck() etc.
			cbFunction(event);
		} else if (window.onEvent) {
			// general callback onEvent() provided to catch all events
			onEvent(event);
		}
	},

// Do XML HTTP request
	_doRequest: function(anEvent, aQuery) {
		// Check if we are not in any error state
		if (PL.state < 0) {
			PL._setStatus('died (' + PL.state + ')');
			return;
		}

		// We may have (async) requests outstanding and thus
		// may have to wait for them to complete and change state.
		var waitForState = false;
		if (anEvent == 'join' || anEvent == 'join-listen') {
			// We can only join after initialization
			waitForState = (PL.state < PL.STATE_READY);
		} else if (anEvent == 'leave') {
			PL.state = PL.STATE_READY;
		} else if (anEvent == 'refresh') {
			// We must be in the listening state
			if (PL.state != PL.STATE_LISTENING) {
				return;
			}
		} else if (anEvent == 'listen') {
			// We must have joined before we can listen
			waitForState = (PL.state < PL.STATE_JOINED);
		} else if (anEvent == 'subscribe' || anEvent == 'unsubscribe') {
			// We must be listeing for subscription mgmnt
			waitForState = (PL.state < PL.STATE_LISTENING);
		} else {
			// All other requests require that we have at least joined
			waitForState = (PL.state < PL.STATE_JOINED);
		}

		// May have to wait for right state to issue request
		if (waitForState == true) {
			PL._setStatus(anEvent + ' , waiting... state=' + PL.state);
			setTimeout(function() {
				PL._doRequest(anEvent, aQuery);
			}, 100);
			return;
		}

		// ASSERTION: PL.state is OK for this request

		// Construct base URL for GET
		var url = PL.pushletURL + '?p_event=' + encodeURIComponent(anEvent);

		// Optionally attach query string
		if (aQuery) {
			url = url + '&' + aQuery;
		}

		// Optionally attach session id
		if (PL.sessionId != null) {
			url = url + '&p_id=' + encodeURIComponent(PL.sessionId);
			if (anEvent == 'p_leave') {
				PL.sessionId = null;
			}
		}
		PL.debug('_doRequest', url);
		PL._getXML(url, PL._onResponse);

		// uncomment to use synchronous XmlHttpRequest
		//var rsp = PL._getXML(url);
		//PL._onResponse(rsp);  */
	},

// Get object reference
	_getObject: function(obj) {
		if (typeof obj == "string") {
			return document.getElementById(obj);
		} else {
			// pass through object reference
			return obj;
		}
	},

	_getWebRoot: function() {
		/** Return directory of this relative to document URL. */
		if (PL.webRoot != null) {
			return PL.webRoot;
		}
		//derive the baseDir value by looking for the script tag that loaded this file
		var head = document.getElementsByTagName('head')[0];
		var nodes = head.childNodes;
		for (var i = 0; i < nodes.length; ++i) {
			var src = nodes.item(i).src;
			if (src) {
				var index = src.indexOf("ajax-pushlet-client.js");
				if (index >= 0) {
					index = src.indexOf("lib");
					PL.webRoot = src.substring(0, index);
					break;
				}
			}
		}
		return PL.webRoot;
	},

// Get XML doc from server
// On response  optional callback fun is called with optional user data.
	_getXML: function(url, callback) {

		// Obtain XMLHttpRequest object
		var xmlhttp = new XMLHttpRequest();
		if (!xmlhttp || xmlhttp == null) {
			alert('No browser XMLHttpRequest (AJAX) support');
			return;
		}

		// Setup optional async response handling via callback
		var cb = callback;
		var async = false;

		if (cb) {
			// Async mode
			async = true;
			xmlhttp.onreadystatechange = function() {
				if (xmlhttp.readyState == 4) {
					if (xmlhttp.status == 200) {
						// Processing statements go here...
						cb(Xparse(xmlhttp.responseText));

						// Avoid memory leaks in IE
						// 12.may.2007 thanks to Julio Santa Cruz
						xmlhttp = null;
					} else {
						var event = new PushletEvent();
						event.put('p_event', 'error')
						event.put('p_reason', '[pushlet] problem retrieving XML data:\n' + xmlhttp.statusText);
						PL._onEvent(event);
					}
				}
			};
		}
		// Open URL
		xmlhttp.open('GET', url, async);  //@wjw_add 为了正确编码,必须使用encodeURI(url)

		// Send XML to KW server
		xmlhttp.send(null);

		if (!cb) {
			if (xmlhttp.status != 200) {
				var event = new PushletEvent();
				event.put('p_event', 'error')
				event.put('p_reason', '[pushlet] problem retrieving XML data:\n' + xmlhttp.statusText);
				PL._onEvent(event)
				return null;
			}
			// Sync mode (no callback)
			// alert(xmlhttp.responseText);

			return Xparse(xmlhttp.responseText);
		}
	},


	_init: function () {
		PL._showStatus();
		PL._setStatus('initializing...');
		/*
			Setup Cross-Browser XMLHttpRequest v1.2
		   Emulate Gecko 'XMLHttpRequest()' functionality in IE and Opera. Opera requires
		   the Sun Java Runtime Environment <http://www.java.com/>.

		   by Andrew Gregory
		   http://www.scss.com.au/family/andrew/webdesign/xmlhttprequest/

		   This work is licensed under the Creative Commons Attribution License. To view a
		   copy of this license, visit http://creativecommons.org/licenses/by-sa/2.5/ or
		   send a letter to Creative Commons, 559 Nathan Abbott Way, Stanford, California
		   94305, USA.

		   */
		// IE support
		if (window.ActiveXObject && !window.XMLHttpRequest) {
			window.XMLHttpRequest = function() {
				var msxmls = new Array(
						'Msxml2.XMLHTTP.5.0',
						'Msxml2.XMLHTTP.4.0',
						'Msxml2.XMLHTTP.3.0',
						'Msxml2.XMLHTTP',
						'Microsoft.XMLHTTP');
				for (var i = 0; i < msxmls.length; i++) {
					try {
						return new ActiveXObject(msxmls[i]);
					} catch (e) {
					}
				}
				return null;
			};
		}

		// ActiveXObject emulation
		if (!window.ActiveXObject && window.XMLHttpRequest) {
			window.ActiveXObject = function(type) {
				switch (type.toLowerCase()) {
					case 'microsoft.xmlhttp':
					case 'msxml2.xmlhttp':
					case 'msxml2.xmlhttp.3.0':
					case 'msxml2.xmlhttp.4.0':
					case 'msxml2.xmlhttp.5.0':
						return new XMLHttpRequest();
				}
				return null;
			};
		}

		PL.pushletURL = 'pushlet.srv';
		PL._setStatus('initialized');
		PL.state = PL.STATE_READY;
	},

/** Handle incoming events from server. */
	_onEvent: function (event) {
		// Create a PushletEvent object from the arguments passed in
		// push.arguments is event data coming from the Server

		PL.debug('_onEvent()', event.toString());

		// Do action based on event type
		var eventType = event.getEvent();

		if (eventType == 'data') {
			PL._setStatus('data');
			PL._doCallback(event, window.onData);
		} else if (eventType == 'refresh') {
			if (PL.state < PL.STATE_LISTENING) {
				PL._setStatus('not refreshing state=' + PL.STATE_LISTENING);
			}
			var timeout = event.get('p_wait');
			setTimeout(function () {
				PL._doRequest('refresh');
			}, timeout);
			return;
		} else if (eventType == 'error') {
			PL.state = PL.STATE_ERROR;
			PL._setStatus('server error: ' + event.get('p_reason'));
			PL._doCallback(event, window.onError);
		} else if (eventType == 'join-ack') {
			PL.state = PL.STATE_JOINED;
			PL.sessionId = event.get('p_id');
			PL._setStatus('connected');
			PL._doCallback(event, window.onJoinAck);
		} else if (eventType == 'join-listen-ack') {
			PL.state = PL.STATE_LISTENING;
			PL.sessionId = event.get('p_id');
			PL._setStatus('join-listen-ack');
			PL._doCallback(event, window.onJoinListenAck);
		} else if (eventType == 'listen-ack') {
			PL.state = PL.STATE_LISTENING;
			PL._setStatus('listening');
			PL._doCallback(event, window.onListenAck);
		} else if (eventType == 'hb') {
			PL._setStatus('heartbeat');
			PL._doCallback(event, window.onHeartbeat);
		} else if (eventType == 'hb-ack') {
			PL._doCallback(event, window.onHeartbeatAck);
		} else if (eventType == 'leave-ack') {
			PL._setStatus('disconnected');
			PL._doCallback(event, window.onLeaveAck);
		} else if (eventType == 'refresh-ack') {
			PL._doCallback(event, window.onRefreshAck);
		} else if (eventType == 'subscribe-ack') {
			PL._setStatus('subscribed to ' + event.get('p_subject'));
			PL._doCallback(event, window.onSubscribeAck);
		} else if (eventType == 'unsubscribe-ack') {
			PL._setStatus('unsubscribed');
			PL._doCallback(event, window.onUnsubscribeAck);
		} else if (eventType == 'abort') {
			PL.state = PL.STATE_ERROR;
			PL._setStatus('abort');
			PL._doCallback(event, window.onAbort);
		} else if (eventType.match(/nack$/)) {
			PL._setStatus('error response: ' + event.get('p_reason'));
			PL._doCallback(event, window.onNack);
		}
	},

/**  Handle XMLHttpRequest response XML. */
	_onResponse: function(xml) {
		PL.debug('_onResponse', xml);
		var events = PL._rsp2Events(xml);
		if (events == null) {
			PL._setStatus('null events')
			return;
		}

		delete xml;

		PL.debug('_onResponse eventCnt=', events.length);
		// Go through all <event/> elements
		for (i = 0; i < events.length; i++) {
			PL._onEvent(events[i]);
		}
	},

/** Convert XML response to PushletEvent objects. */
	_rsp2Events: function(xml) {
		// check empty response or xml document
		if (!xml || !xml.contents) {
			return null;
		}

		// Convert xml doc to array of PushletEvent objects
		var eventElements = xml.contents[0].contents;  // XML document is parsed into tree of arrays, contents[0] is <pushlet> element
		var events = new Array(eventElements.length);
		for (i = 0; i < eventElements.length; i++) {
			events[i] = new PushletEvent(eventElements[i]);
		}

		return events;

	},

	statusMsg: 'null',
	statusChanged: false,
	statusChar: '|',


	_showStatus: function() {
		// To show progress
		if (PL.statusChanged == true) {
			if (PL.statusChar == '|') PL.statusChar = '/';
			else if (PL.statusChar == '/') PL.statusChar = '--';
			else if (PL.statusChar == '--') PL.statusChar = '\\';
			else PL.statusChar = '|';
			PL.statusChanged = false;
		}
		window.defaultStatus = PL.statusMsg;
		window.status = PL.statusMsg + '  ' + PL.statusChar;
		timeout = window.setTimeout('PL._showStatus()', 400);
	},

	_setStatus: function(status) {
		PL.statusMsg = "pushlet - " + status;
		PL.statusChanged = true;
	},



/*************** Debug utility *******************************/
	timestamp: 0,
	debugWindow: null,
	messages: new Array(),
	messagesIndex: 0,
	debugOn: false,

/** Send debug messages to a (D)HTML window. */
	debug: function(label, value) {
		if (PL.debugOn == false) {
			return;
		}
		var funcName = "none";

		// Fetch JS function name if any
		if (PL.debug.caller) {
			funcName = PL.debug.caller.toString()
			funcName = funcName.substring(9, funcName.indexOf(")") + 1)
		}

		// Create message
		var msg = "-" + funcName + ": " + label + "=" + value

		// Add optional timestamp
		var now = new Date()
		var elapsed = now - PL.timestamp
		if (elapsed < 10000) {
			msg += " (" + elapsed + " msec)"
		}

		PL.timestamp = now;

		// Show.

		if ((PL.debugWindow == null) || PL.debugWindow.closed) {
			PL.debugWindow = window.open("", "p_debugWin", "toolbar=no,scrollbars=yes,resizable=yes,width=600,height=400");
		}

		// Add message to current list
		PL.messages[PL.messagesIndex++] = msg

		// Write doc header
		PL.debugWindow.document.writeln('<html><head><title>Pushlet Debug Window</title></head><body bgcolor=#DDDDDD>');

		// Write the messages
		for (var i = 0; i < PL.messagesIndex; i++) {
			PL.debugWindow.document.writeln('<pre>' + i + ': ' + PL.messages[i] + '</pre>');
		}

		// Write doc footer and close
		PL.debugWindow.document.writeln('</body></html>');
		PL.debugWindow.document.close();
		PL.debugWindow.focus();

	}


}


/* Represents nl.justobjects.pushlet.Event in JS. */
function PushletEvent(xml) {
	// Member variable setup; the assoc array stores the N/V pairs
	this.arr = new Array();

	this.getSubject = function() {
		return this.get('p_subject');
	}

	this.getEvent = function() {
		return this.get('p_event');
	}

	this.put = function(name, value) {
		return this.arr[name] = value;
	}

	this.get = function(name) {
		return this.arr[name];
	}

	this.toString = function() {
		var res = '';
		for (var i in this.arr) {
			res = res + i + '=' + this.arr[i] + '\n';
		}
		return res;
	}

	this.toTable = function() {
		var res = '<table border="1" cellpadding="3">';
		var styleDiv = '<div style="color:black; font-family:monospace; font-size:10pt; white-space:pre;">'

		for (var i in this.arr) {
			res = res + '<tr><td bgColor=white>' + styleDiv + i + '</div></td><td bgColor=white>' + styleDiv + this.arr[i] + '</div></td></tr>';
		}
		res += '</table>'
		return res;
	}

	// Optional XML element <event name="value" ... />
	if (xml) {
    // Put the attributes in Map
		for (var p in xml.attributes) {
      this.put(p, xml.attributes[p]);
	  }
	}
}

function isArray(it){
      return {}.toString.call(it) == "[object Array]";
}

function objectToQuery(/*Object*/ map){
    // summary:
    //    takes a name/value mapping object and returns a string representing
    //    a URL-encoded version of that object.
    // example:
    //    this object:
    //
    //  | {
    //  |   blah: "blah",
    //  |   multi: [
    //  |     "thud",
    //  |     "thonk"
    //  |   ]
    //  | };
    //
    // yields the following query string:
    //
    //  | "blah=blah&multi=thud&multi=thonk"

    // FIXME: need to implement encodeAscii!!
    var enc = encodeURIComponent, pairs = [];
    for(var name in map){
        var value = map[name];
        var assign = enc(name) + "=";
        if(isArray(value)){
            for(var i = 0, l = value.length; i < l; ++i){
                pairs.push(assign + enc(value[i]));
            }
        }else{
            pairs.push(assign + enc(value));
        }
    }
    return pairs.join("&"); // String
}

function queryToObject(/*String*/ str){
    // summary:
    //    Create an object representing a de-serialized query section of a
    //    URL. Query keys with multiple values are returned in an array.
    //
    // example:
    //    This string:
    //
    //  |   "foo=bar&foo=baz&thinger=%20spaces%20=blah&zonk=blarg&"
    //
    //    results in this object structure:
    //
    //  |   {
    //  |     foo: [ "bar", "baz" ],
    //  |     thinger: " spaces =blah",
    //  |     zonk: "blarg"
    //  |   }
    //
    //    Note that spaces and other urlencoded entities are correctly
    //    handled.

    // FIXME: should we grab the URL string if we're not passed one?
    var dec = decodeURIComponent, qp = str.split("&"), ret = {}, name, val;
    for(var i = 0, l = qp.length, item; i < l; ++i){
        item = qp[i];
        if(item.length){
            var s = item.indexOf("=");
            if(s < 0){
                name = dec(item);
                val = "";
            }else{
                name = dec(item.slice(0, s));
                val  = dec(item.slice(s + 1));
            }
            if(typeof ret[name] == "string"){ // inline'd type check
                ret[name] = [ret[name]];
            }

            if(isArray(ret[name])){
                ret[name].push(val);
            }else{
                ret[name] = val;
            }
        }
    }
    return ret; // Object
}


/**********************************************************************
 START - OLD application functions (LEFT HERE FOR FORWARD COMPAT)
 ***********************************************************************/
// Debug util
function p_debug(aBool, aLabel, aMsg) {
	if (aBool == false) {
		return;
	}

	PL.setDebug(true);
	PL.debug(aLabel, aMsg);
	PL.setDebug(false);
}

// Embed pushlet frame in page (OBSOLETE)
function p_embed(thePushletWebRoot) {
	alert('Pushlets: p_embed() is no longer required for AJAX client')
}

// Join the pushlet server
function p_join(aSession_id) {
	PL.join(aSession_id);
}

// Create data event channel with the server
function p_listen(aSubject, aMode) {
	// Note: mode is fixed to 'pull'
	PL.listen(aSubject);
}

// Shorthand: Join the pushlet server and start listening immediately
function p_join_listen(aSubject,aSession_id) {
	PL.joinListen(aSubject,aSession_id);
}

// Leave the pushlet server
function p_leave() {
	PL.leave();
}

// Send heartbeat event; callback is onHeartbeatAck()
function p_heartbeat() {
	PL.heartbeat();
}

// Publish to a subject
function p_publish(aSubject, nvPairs) {
	var args = p_publish.arguments;

	// Put the arguments' name/value pairs in the URI
	var query = '';
	var amp = '';
	for (var i = 1; i < args.length; i++) {
		if (i > 1) {
			amp = '&';
		}
		query = query + amp + args[i] + '=' + encodeURIComponent(args[++i]);
	}
	PL.publish(aSubject, query);
}

// Publish to a subject 给在线用户.
function p_publish_to_online(aSubject, nvPairs) {
  var args = p_publish.arguments;

  // Put the arguments' name/value pairs in the URI
  var query = '';
  var amp = '';
  for (var i = 1; i < args.length; i++) {
    if (i > 1) {
      amp = '&';
    }
    query = query + amp + args[i] + '=' + encodeURIComponent(args[++i]);
  }
  PL.publish_to_online(aSubject, query);
}

// Subscribe to a subject with optional label
function p_subscribe(aSubject, aLabel) {
	PL.subscribe(aSubject, aLabel);
}

// Unsubscribe from a subject
function p_unsubscribe(aSid) {
	PL.unsubscribe(aSid);
}

/**********************************************************************
 END - Public application functions (LEFT HERE FOR FORWARD COMPAT)
 ***********************************************************************/

// Initialize when page completely loaded
//@wjw_comment 不要自动初始化,跟ext等会有冲突 PL._addEvent(window, 'load', PL._init, false);

