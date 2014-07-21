var	url = require('url');
var http = require('http');
var wisdmconfig=require('./wisdmconfig').wisdmconfig;	
var common=require('./../processingnodeclient/src/common.js').common;
var temporaryfileserver=require('./temporaryfileserver').temporaryfileserver;
var WISDMUSAGE=require('../processingnodeclient/src/wisdmusage').WISDMUSAGE;
WISDMUSAGE.startPeriodicWritePendingRecords();
WISDMUSAGE.setCollectionName('processingwebserver');
var SessionHandler=require('./sessionhandler.js').SessionHandler;
var JobManager=require('./jobmanager').JobManager;
	
function on_request(request,callback) {
	console.log ('REQUEST:::: '+request.service+' '+request.command);
	
	request.auth_info={};
	if (request.browser_code) {
		var url=wisdmconfig.processingwebserver.wisdmserver_url+'/wisdmserver?';
		url+='service=authentication&';
		url+='command=getAuthInfo&';
		url+='browser_code='+request.browser_code+'&';
		get_json(url,function(tmp) {
			if (!tmp.success) {
				console.error('Error in getAuthInfo...',tmp.error,url);
			}
			else {
				request.auth_info=tmp;
			}
			on_request_part2();
		});
	}
	else on_request_part2();
	
	function on_request_part2() {
		
		var user_id=(request.auth_info||{}).user_id;
		WISDMUSAGE.addRecord({
			user_id:user_id,
			usage_type:'request_bytes',
			amount:JSON.stringify(request).length,
			name:request.command||''
		});
		
		
		var service=request.service||'';
		
		if (service=='processing') {
			processing(request,function(resp) {
				if (('data_base64' in resp)&&(resp.data_base64.length>1000)) {
					var data_base64=resp.data_base64;
					make_data_base64_url(data_base64,function(tmp1) {
						if (!tmp1.success) {
							console.error('Problem making data_base64_url');
							finalize(resp);
							return;
						}
						resp.data_base64_url=tmp1.url;
						delete(resp.data_base64);
						finalize(resp);
					});
				}
				else finalize(resp);
			});
		}
		else if (service=='temporaryfileserver') {
			temporaryfileserver(request,finalize);
		}
		else {
			finalize({success:false,error:'Unknown service: '+service});
		}
		
		function finalize(tmp) {
			
			WISDMUSAGE.addRecord({
				user_id:user_id,
				usage_type:'response_bytes',
				amount:JSON.stringify(tmp).length,
				name:request.command||''
			});
			if (callback) callback(tmp);
		}
	}
}

function make_data_base64_url(data_base64,callback) {
	var path=wisdmconfig.processingwebserver.www_path+'/data_base64';
	common.mkdir(path,function() {
		var checksum=compute_sha1(data_base64);
		fs.writeFile(path+'/'+checksum+'.txt',data_base64,function(err) {
			if (err) {
				callback({success:false,error:'Error writing data.'});
				return;
			}
			callback({success:true,url:wisdmconfig.processingwebserver.processingwebserver_url+'/data_base64/'+checksum+'.txt'});
		});
	});
}

function compute_sha1(data) {
	var crypto=require('crypto');
	var ret=crypto.createHash('sha1');
	ret.update(data);
	return ret.digest('hex');
}

function get_json(url,callback) {
	http.get(url,function(resp) {
		var body='';
		resp.on('data',function(chunk) {
			body+=chunk;
		});
		resp.on('end',function() {
			var ret={};
			try {
				ret=JSON.parse(body);
			}
			catch(err) {
				callback({success:false,error:'Problem parsing json response: '+body});
				return;
			}
			ret.success=true;
			callback(ret);
		});
	}).on('error',function(err) {
		console.error('Error in get_json:',url,err);
		callback({success:false,error:err});
	});
}

var static0=require('node-static');
var fileServer=new static0.Server(wisdmconfig.processingwebserver.www_path,{cache:3600});
	
http.createServer(function (REQ, RESP) {
	if (REQ.method == 'OPTIONS') {
		var headers = {};
		// IE8 does not allow domains to be specified, just the *
		// headers["Access-Control-Allow-Origin"] = req.headers.origin;
		headers["Access-Control-Allow-Origin"] = "*";
		headers["Access-Control-Allow-Methods"] = "POST, GET, PUT, DELETE, OPTIONS";
		headers["Access-Control-Allow-Credentials"] = false;
		headers["Access-Control-Max-Age"] = '86400'; // 24 hours
		headers["Access-Control-Allow-Headers"] = "X-Requested-With, X-HTTP-Method-Override, Content-Type, Accept";
		RESP.writeHead(200, headers);
		RESP.end();
	}
	else if(REQ.method=='POST') {
		var body='';
		REQ.on('data', function (data) {
			body+=data;
		});
		REQ.on('end',function(){
			var POST =  body;
			var request0;
			try {
				request0=JSON.parse(POST);
			}
			catch(err) {
				console.error(JSON.stringify(err));
				error_log(JSON.stringify(err));
				send_response({success:false,error:JSON.stringify(err)});
				return;
			}
			on_request(request0,function(resp) {
				send_response(resp);
			});
		});
	}
	else if(REQ.method=='GET') {
		var url_parts = url.parse(REQ.url,true);
		if (url_parts.pathname=='/processingwebserver') {
			on_request(url_parts.query,function(resp) {
				send_response(resp);
			});
		}
		else {
			//question: why do they have the .addListener functionality in the example on node.js?
			//REQ.addListener('end',function() {
			RESP.setHeader("Access-Control-Allow-Origin", "*");
			fileServer.serve(REQ,RESP,function(err,result) {
			});
			
			var previous_bytes_written=(REQ.socket||{}).bytesWritten||0;
			RESP.on('finish',function() {
				var bytes_written=(REQ.socket||{}).bytesWritten||0;
				bytes_written-=previous_bytes_written;
				if (bytes_written>0) {
					WISDMUSAGE.addRecord({
						user_id:'',
						usage_type:'file_server_bytes',
						amount:bytes_written,
						name:''
					});
				}
			});
			
			//});
		}
	}
	
	function send_response(obj) {
		if (!obj.response_type) {
			RESP.writeHead(200, {"Access-Control-Allow-Origin":"*", "Content-Type":"application/json"});
			RESP.end(JSON.stringify(obj));
		}
		else if (obj.response_type=='download') {
			RESP.writeHead(200, {"Access-Control-Allow-Origin":"*", "Content-Type":obj.contentType});
			RESP.end(obj.content);
		}
	}
	
}).listen(wisdmconfig.processingwebserver.listen_port);

var fs = require('fs');
var ProcessingNodeServer=require('./processingnodeserver').ProcessingNodeServer;

function has_permission_to_access_node(NN,user_id,permissions) {
	if ((NN.owner()==user_id)&&(user_id)) return true; //owner may access always
	var access=NN.processingNodeAccess();
	if (!access.users) access.users=[];
	var ret=false;
	access.users.forEach(function(user) {
		if ((user.user_id==user_id)&&(user_id)) {
			ret=true; //can't just return here because we are in another function!
		}
		if ((user.user_id||'')=='public') {
			ret=true;
		}
	});
	return ret;
}


function processing(request,callback) {
	var user_id=(request.auth_info||{}).user_id;
	var permissions=(request.auth_info||{}).permissions||{};
	var processing_node_id=request.processing_node_id||'';
	var command=request.command||'';
	
	if (command=='getConnectedProcessingNodeIds') {
		var ids=NODESERVER.getConnectedProcessingNodeIds();
		callback({success:true,ids:ids});
		return;
	}
	
	if (command=='updateProcessingNodeSource') {
		if (user_id!='magland') {
			callback({success:false,error:'You are not authorized to update the processing node source',authorization_error:true});
			return;
		}
		if (request.processing_node_id=='*') {
			var pnids=NODESERVER.getConnectedProcessingNodeIds();
			common.for_each_async(pnids,function(pnid,cb) {
				var NN=NODESERVER.findProcessingNodeConnection(pnid);
				if (!NN) {
					cb({success:false,error:'Unable to find processing node connection: '+pnid});
					return;
				}
				NN.processRequest(request,function(tmp002) {
					cb(tmp002);
				});
			},function(tmp1) {
				callback(tmp1);
			},5);
		}
		else {
			var NN=NODESERVER.findProcessingNodeConnection(request.processing_node_id);
			if (!NN) {
				callback({success:false,error:'Unable to find processing node connection: '+request.processing_node_id});
				return;
			}
			NN.processRequest(request,function(tmp002) {
				callback(tmp002);
			});
		}
		return;
	}
	
	if (command=='addApprovedProcessingNode') {
		NODESERVER.addApprovedProcessingNode(request,callback);
		return;
	}
	
	if (command=='setSessionParameters') {
		SESSIONHANDLER.setSessionParameters(request.session_id,request.parameters);
		callback({success:true});
		return;
	}
	if (command=='getSessionSignals') {
		SESSIONHANDLER.getSessionSignals(request.session_id,function(tmp) {
			callback(tmp);
		},2000);
		return;
	}
	if ((command=='submitScript')&&(request.jobKey)) {
		request.callback=callback;
		JOBMANAGER.addJob(request);
		return;
	}
	
	if (!processing_node_id) {
		callback({success:false,error:'Missing parameter: processing_node_id'});
		return;
	}
	var NN=NODESERVER.findProcessingNodeConnection(processing_node_id);
	if (!NN) {
		callback({success:false,error:'Unable to find processing node: '+processing_node_id});
		return;
	}
	
	if ((request.command||'')=='checkNodeConnected') {
		callback({success:true});
		return;
	}
	
	if ((request.command||'')=='getProcessingNodeAccess') {
		if ((NN.owner()!=user_id)||(!user_id)) {
			callback({success:false,error:'You are not authorized to get this information.'});
			return;
		}
		callback({success:true,access:NN.processingNodeAccess(),owner:NN.owner()});
		return;
	}
	
	if ((request.command||'')=='setProcessingNodeAccess') {
		if ((NN.owner()!=user_id)||(!user_id)) {
			callback({success:false,error:'You are not authorized to set this information.'});
			return;
		}
		callback({success:true});
	}
	
	if (!has_permission_to_access_node(NN,user_id,permissions)) {
		callback({success:false,error:'You do not have permission to access this processing node: '+processing_node_id});
		return;
	}
	
	NN.processRequest(request,callback);
}

var SESSIONHANDLER=new SessionHandler();

var NODESERVER=new ProcessingNodeServer();
NODESERVER.onSignal(function(parameters,signal) {
	SESSIONHANDLER.addSignal(parameters,signal);
});

NODESERVER.startListening(wisdmconfig.processingnodeserver.listen_port);
var JOBMANAGER=new JobManager(NODESERVER);

