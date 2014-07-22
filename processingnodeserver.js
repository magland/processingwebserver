var WisdmSocket=require('./wisdmsocket').WisdmSocket;
var wisdmconfig=require('./wisdmconfig').wisdmconfig;
var DATABASE=require('./databasemanager').DATABASE;

function ProcessingNodeServer() {
	var that=this;
	
	this.startListening=function(port) {m_server.listen(port); _initialize();};
	this.getConnectedProcessingNodeIds=function() {return _getConnectedProcessingNodeIds();};
	this.findProcessingNodeConnection=function(id) {return _findProcessingNodeConnection(id);};
	this.addApprovedProcessingNode=function(request,callback) {return _addApprovedProcessingNode(request,callback);};
	this.onSignal=function(handler) {m_signal_handlers.push(handler);};
	
	var m_approved_processing_nodes={};
	var m_node_connections={};
	var m_server=null;
	var m_signal_handlers=[];
	
	function _findProcessingNodeConnection(id) {
		if (id in m_node_connections) {
			if (m_node_connections[id].isConnected()) {
				return m_node_connections[id]; 
			}
			else {
				delete m_node_connections[id];
				return null;
			}
		}
		else return null;
	}
	function _getConnectedProcessingNodeIds() {
		var ret=[];
		for (var id in m_node_connections) {
			if (m_node_connections[id].isConnected()) {
				ret.push(id);
			}
		}
		ret.sort();
		return ret;
	}
	
	m_server=require('net').createServer(function(socket) {
		
		var wsocket=new WisdmSocket(socket);
		
		var node_connection=null;
		var processing_node_id=null;
		
		var initialized=false;
		wsocket.onMessage(function (msg) {
			if (!initialized) {
				if (msg.command=='connect_as_processing_node') {
					if (is_valid_connection_request(msg)) {
						if (msg.processing_node_id in m_node_connections) {
							if (!m_node_connections[msg.processing_node_id].isConnected()) {
								console.log ('REMOVING NODE CONNECTION **: '+msg.processing_node_id);
								delete m_node_connections[msg.processing_node_id];
							}
						}
						if (!(msg.processing_node_id in m_node_connections)) {
							processing_node_id=msg.processing_node_id;
							node_connection=new ProcessingNodeConnection();
							node_connection.setWisdmSocket(wsocket);
							node_connection.setProcessingNodeId(processing_node_id);
							node_connection.setOwner(msg.owner);
							node_connection.setJobKeys(msg.job_keys||[]);
							node_connection.setProcessingNodeAccess(msg.access||{error:'Undefined access'});
							node_connection.onSignal(function(signal) {
								var ppp={processing_node_id:processing_node_id};
								m_signal_handlers.forEach(function(handler) {
									handler(ppp,signal);
								});
							});
							
							m_node_connections[processing_node_id]=node_connection;
							node_connection.initialize();
							initialized=true;
							
							console.log ('PROCESSING NODE CONNECTED: '+msg.processing_node_id);
							/*
							setTimeout(function() {
								test_get_file_bytes();
							},1000);
							*/
							
						}
						else {
							console.error('A node with this id is already connected: '+msg.processing_node_id);
							close_socket();
						}
					}
					else {
						console.error('Rejecting invalid connection request: '+msg.processing_node_id+' '+msg.owner);
						close_socket();
					}
				}
				else {
					console.error('Expected command=connect_as_processing_node');
					close_socket();
				}
			}
		});
		wsocket.onClose(function() {
			do_cleanup();
		});
		
		function close_socket() {
			if (!wsocket) return;
			console.error('closing socket: '+wsocket.remoteAddress()+":"+wsocket.remotePort());
			wsocket.disconnect();
			wsocket=null;
			if ((processing_node_id)&&(processing_node_id in m_node_connections)) {
				m_node_connections[processing_node_id]=null;
			}
		}
	});
	
	function _initialize() {
		var DB=DATABASE('processingnodeserver');
		DB.setCollection('approved_processing_nodes');
		DB.find({},{owner:1,processing_node_id:1,secret_id:1},function(err,docs) {
			if (err) {
				console.error('Problem finding approved nodes: '+err);
				return;
			}
			docs.forEach(function(doc) {
				m_approved_processing_nodes[doc.processing_node_id]=doc;
			});
		});
	}
	
	function _addApprovedProcessingNode(request,callback) {
		var processing_node_id=request.processing_node_id;
		var user_id=(request.auth_info||{}).user_id;
		var owner=request.owner||'';
		var secret_id=request.secret_id||'';
		
		if ((!processing_node_id)||(!secret_id)||(!owner)) {
			callback({success:false,error:'Missing required information.'});
			return;
		}
		if (processing_node_id.length>100) {
			callback({success:false,error:'processing_node_id is too long!'});
			return;
		}
		if (secret_id.length>100) {
			callback({success:false,error:'secret_id is too long!'});
			return;
		}
		
		if (user_id!=owner) {
			callback({success:false,error:'user_id does not match owner'});
			return;
		}
		
		var DB=DATABASE('processingnodeserver');
		DB.setCollection('approved_processing_nodes');
		
		DB.find({_id:processing_node_id},{owner:1},function(err,docs) {
			if (err) {
				callback({success:false,error:'Problem in find: '+err});
				return;
			}
			if (docs.length>0) {
				if (docs[0].owner!=owner) {
					callback({success:false,error:'processing_node_id exists with different owner'});
					return;
				}
			}
			var doc={_id:processing_node_id,processing_node_id:processing_node_id,owner:owner,secret_id:secret_id};
			DB.save(doc,function(err) {
				if (err) {
					callback({success:false,error:'Error saving record to database: '+err});
					return;
				}
				m_approved_processing_nodes[processing_node_id]=doc;
				callback({success:true});
			});
		});
	}
	
	/*
	function open_database(params,callback) {
		var db=new mongo.Db('processingnodeserver', new mongo.Server('localhost',params.port||27017, {}), {safe:true});
		db.open(function(err,db) {
			if (err) {
				if (callback) callback(err,null);
			}
			else {
				if (callback) callback('',db);
			}
		});
	}
	*/
	
	function is_valid_connection_request(msg) {
		var processing_node_id=msg.processing_node_id;
		if (!(processing_node_id in m_approved_processing_nodes)) return false;
		var tmp=m_approved_processing_nodes[processing_node_id];
		if (tmp.processing_node_id!=msg.processing_node_id) return false;
		if (tmp.owner!=msg.owner) return false;
		if (tmp.secret_id!=msg.secret_id) return false;
		return true;
	}
	
	function do_cleanup() {
		for (var id in m_node_connections) {
			if (!m_node_connections[id].isConnected()) {
				console.log ('REMOVING NODE CONNECTION: '+id);
				delete m_node_connections[id];
			}
		}
	}
	
	function periodic_cleanup() {
		do_cleanup();
		
		setTimeout(periodic_cleanup,10000);
	}
	periodic_cleanup();
}

function ProcessingNodeConnection() {
	var that=this;
	
	this.setWisdmSocket=function(wsocket) {m_wsocket=wsocket;};
	this.initialize=function() {_initialize();};
	this.processRequest=function(request,callback) {_processRequest(request,callback);};
	this.isConnected=function() {return _isConnected();};
	this.setProcessingNodeId=function(id) {m_processing_node_id=id;};
	this.setOwner=function(owner) {m_owner=owner;};
	this.setProcessingNodeAccess=function(access) {m_processing_node_access=access;};
	this.processingNodeId=function() {return m_processing_node_id;};
	this.owner=function() {return m_owner;};
	this.processingNodeAccess=function() {return m_processing_node_access;};
	this.onSignal=function(handler) {m_signal_handlers.push(handler);};
	this.setJobKeys=function(job_keys) {m_job_keys=job_keys;};
	this.jobKeys=function() {return m_job_keys;};
	
	var m_wsocket=null;
	var m_response_waiters={};
	var m_processing_node_id='';
	var m_owner='';
	var m_processing_node_access={};
	var m_signal_handlers=[];
	var m_job_keys=[];
	
	function _initialize() {
		if (!m_wsocket) return;
		m_wsocket.sendMessage({command:'connection_accepted'});
		m_wsocket.onMessage(function(msg) {
			process_message_from_node(msg);
		});
	}
	
	function checkRequestAllowed(request,callback) {
		/*var valid_node_commands=[
			'checkNodeStatus',
			'submitScript','getFileChecksum','getFileText','setFileText','getFileData','setFileData',
			'getFileNames','getFolderNames','removeFile','getFileBytes','getProcessingSummary','find',
			'removeNonfinishedProcesses'
		];*/
		var valid_node_commands=[
			'checkNodeStatus',
			'submitScript','getFileChecksum','getFileText','setFileText','getFileData','setFileData',
			'getFileBytes','getProcessingSummary','find','removeNonfinishedProcesses','updateProcessingNodeSource',
			'getProcessingNodeAccess','setProcessingNodeAccess'
		];
		//TODO: change find to findProcesses?
		var command=request.command||'';
		var auth_info=request.auth_info||{};
		if (!auth_info.permissions) auth_info.permissions={};
		if (valid_node_commands.indexOf(command)<0) {
			callback({allowed:false,reason:'unknown',message:'Unknown command ***: '+command});
			return;
		}
		
		if (command=='submitScript') {
			if (!auth_info.permissions.submit_script) {
				callback({allowed:false,reason:'unauthorized',message:'You are not authorized to submit scripts.'});
				return;
			}
		}
		else if (command=='removeNonfinishedProcesses') {
			if (!auth_info.permissions.submit_script) {
				callback({allowed:false,reason:'unauthorized',message:'You are not authorized to remove processes.'});
				return;
			}
		}
		else if (command=='updateProcessingNodeSource') {
			if ((auth_info||{}).user_id!='magland') {
				callback({allowed:false,reason:'unauthorized',message:'You are not authorized to update processing node source.'});
				return;
			}
		}
		callback({allowed:true});
		
	}
	
	function _processRequest(request,callback) {
		checkRequestAllowed(request,function(tmp00) {
			if (tmp00.allowed) {
				var command=request.command||'';
				if (command=='checkNodeStatus') {
					callback({success:true,status:'found'});
				}
				else {
					send_request_to_node(request,callback);
				}
			}
			else {
				if (tmp00.reason=='unauthorized') {
					callback({success:false,error:tmp00.message,authorization_error:true});
				}
				else {
					callback({success:false,error:tmp00.message});
				}
			}
		});
	}
	function _isConnected() {
		if (!m_wsocket) return;
		return m_wsocket.isConnected();
	}
	function make_random_id(numchars) {
		if (!numchars) numchars=10;
		var text = "";
		var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
		for( var i=0; i < numchars; i++ ) text += possible.charAt(Math.floor(Math.random() * possible.length));	
		return text;
	} 
	function send_request_to_node(request,callback) {
		request.server_request_id=make_random_id(8);
		if (m_wsocket) {
			m_wsocket.sendMessage(request);
			m_response_waiters[request.server_request_id]=function(tmpCC) {
				if (tmpCC.success) {
					if ((request.command||'')=='setProcessingNodeAccess') {
						m_processing_node_access=request.access||{error:'Unexpected problem 343'};
					}
				}
				callback(tmpCC);
			};
		}
		else {
			console.error('Could not send request to node... m_wsocket is null');
		}
	}
	
	function process_message_from_node(msg,callback) {
		if (msg.server_request_id) {
			if (msg.server_request_id in m_response_waiters) {
				m_response_waiters[msg.server_request_id](msg);
				delete m_response_waiters[msg.server_request_id];
			}
		}
		else if ((msg.command||'')=='signal') {
			m_signal_handlers.forEach(function(handler) {
				handler(msg);
			});
		}
	}
}

exports.ProcessingNodeServer=ProcessingNodeServer;