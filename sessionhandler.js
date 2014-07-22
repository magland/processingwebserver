var common=require('./common').common;

function SessionHandler() {
	var that=this;
	
	this.setSessionParameters=function(session_id,parameters) {_setSessionParameters(session_id,parameters);};
	this.getSessionSignals=function(session_id,callback,timeout) {_getSessionSignals(session_id,callback,timeout);};
	this.addSignal=function(parameters,signal) {_addSignal(parameters,signal);};
	
	var m_sessions={};
	
	function _setSessionParameters(session_id,parameters) {
		if (!(session_id in m_sessions)) {
			m_sessions[session_id]=make_new_session();
		}
		m_sessions[session_id].parameters=JSON.parse(JSON.stringify(parameters));
		m_sessions[session_id].timestamp=new Date();
	}
	function make_new_session() {
		var ret={};
		ret.parameters={};
		ret.signals=[];
		ret.signals_added_handlers=[];
		ret.timestamp=new Date();
		return ret;
	}
	function _getSessionSignals(session_id,callback,timeout) {
		var finalized=false;
		
		if (!(session_id in m_sessions)) {
			callback({success:false,error:'Session not found: '+session_id});
			return;
		}
		var S0=m_sessions[session_id];
		S0.timestamp=new Date();
		if (S0.signals.length>0) {
			do_finalize();
			return;
		}
		var tmp_id=common.make_random_id(10);
		S0.signals_added_handlers.push(function() {
			setTimeout(function() {
				do_finalize();
			},200);
		});
		setTimeout(function() {
			do_finalize();
		},timeout);
		
		function do_finalize() {
			if (finalized) return;
			finalized=true;
			callback({success:true,signals:S0.signals});
			S0.signals=[];
		}
	}
	function _addSignal(parameters,signal) {
		for (var session_id in m_sessions) {
			var S0=m_sessions[session_id];
			if (signal_matches(parameters,S0.parameters)) {
				S0.signals.push(signal);
				for (var ii=0; ii<S0.signals_added_handlers.length; ii++) {
					S0.signals_added_handlers[ii]();
				}
				S0.signals_added_handlers=[];
			}
		}
	}
	function signal_matches(signal_parameters,session_parameters) {
		if (signal_parameters.processing_node_id==session_parameters.processing_node_id) {
			return true;
		}
		return false;
	}
	function periodic_cleanup() {
		var num_sessions=0;
		var num_removed=0;
		for (var session_id in m_sessions) {
			num_sessions++;
			var elapsed=(new Date())-m_sessions[session_id].timestamp;
			if (elapsed>3*60*1000) {
				num_removed++;
				delete m_sessions[session_id];
			}
		}
		setTimeout(periodic_cleanup,5000);
	}
	periodic_cleanup();
}
exports.SessionHandler=SessionHandler;