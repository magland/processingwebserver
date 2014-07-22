var common=require('./common.js').common;
exports.JobManager=JobManager;

function JobManager(NODESERVER) {
	var that=this;
	
	this.addJob=function(params) {_addJob(params);};
	
	var m_jobs={};
	
	function _addJob(params) {
		params.job_id=make_random_id(10);
		params.status='queued';
		m_jobs[params.job_id]=params;
	}
	function on_timer() {
		for (var id in m_jobs) {
			check_job(id);
		}
		setTimeout(on_timer,2000);		
	}
	on_timer();
	
	function check_job(job_id) {
		var job0=m_jobs[job_id]||{};
		var status=job0.status||'';
		if (status=='queued') {
			check_queued_job(job0);
		}
		else if (status=='running') {
			check_running_job(job0);
		}
		else if (status=='completed') {
			check_completed_job(job0);
		}
		else {
			console.error('Unexpected problem: unknown status for job, removing: '+status+', '+job0.job_id);
			if (job0.job_id) delete m_jobs[job0.job_id];
		}
	}
	
	function check_completed_job(job0) {
		var elapsed=(new Date())-job0.completed_timestamp;
		if (elapsed>60*1000) {
			delete m_jobs[job0.job_id];
		}
	}
	
	function check_queued_job(job0) {
		var node_id=find_node_id_for_job_key(job0.jobKey);
		console.error('Unable to find node id for job key: '+job0.jobKey+'.');
		if (!node_id) return;
		job0.processing_node_id=node_id;
		job0.status='running';
		job0.statusDetail='submitted';
		var NN=NODESERVER.findProcessingNodeConnection(node_id);
		if (NN) {
			console.log ('Launching script on node: '+node_id);
			NN.processRequest(job0,function(tmp) {
				if (job0.callback) job0.callback(tmp);
				if (!tmp.success) {
					job0.status='completed';
					job0.completed_timestamp=new Date();
					job0.statusDetail='';
					job0.error=tmp.error;
					console.error('Error launching script: '+tmp.error);
					return;
				}
				job0.statusDetail='';
				job0.submitted_processes=tmp.submitted_processes||[];
			});
		}
		else {
			job0.status='error';
			job0.statusDetail='';
			job0.error='Unexpected problem 74';
		}
	}
	function find_node_id_for_job_key(job_key) {
		var running_node_ids={};
		for (var job_id in m_jobs) {
			var job0=m_jobs[job_id];
			if (job0.status=='running') running_node_ids[job0.processing_node_id]=1;
		}
		var ids=NODESERVER.getConnectedProcessingNodeIds();
		for (var i in ids) {
			var id=ids[i];
			if (!(id in running_node_ids)) {
				var NN=NODESERVER.findProcessingNodeConnection(id);
				if (NN) {
					var keys=NN.jobKeys();
					if (keys.indexOf(job_key)>=0) return id;
				}
				else {
					console.error('Unexpected problem finding node connection: '+id);
				}
			}
		}
		return null;
	}
	function check_running_job(job0) {
		if (job0.statusDetail!=='') return;
		var NN=NODESERVER.findProcessingNodeConnection(job0.processing_node_id);
		if (!NN) {
			job0.status='error';
			job0.statusDetail='';
			job0.error='Unable to find node of running process';
			console.error('Unable to find node of running process: '+job0.processing_node_id);
			return;
		}
		if (job0.statusDetail!=='') return;
		job0.statusDetail='checking_processing_complete';
		check_processing_complete(NN,job0,function(tmp1) {
			job0.statusDetail='';
			if (tmp1.complete) {
				console.log ('JOB COMPLETED: '+job0.processing_node_id+'.');
				if (job0.error) console.err(job0.error);
				job0.status='completed';
				job0.completed_timestamp=new Date();
			}
		});
	}
	
	function check_processing_complete(NN,job0,callback) {
		
		var processes_to_check=[];
		for (var i=0; i<job0.submitted_processes.length; i++) {
			var PP=job0.submitted_processes[i];
			var need_to_check=true;
			if (PP.previous_status=='finished') need_to_check=false;
			if ((PP.status||'')=='finished') need_to_check=false;
			if (PP.status=='error') {
				job0.error='Processing has one or more errors.';
				need_to_check=false;
			}
			if (need_to_check) processes_to_check.push(PP);
		}
		if (processes_to_check.length===0) {
			callback({complete:true});
			return;
		}
		var process_ids_to_check=[];
		processes_to_check.forEach(function(PP) {
			process_ids_to_check.push(PP.process_id);
		});
		var req0={
			service:'processing',
			processing_node_id:job0.processing_node_id,
			command:'find',
			collection:'processes',
			query:{_id:{$in:process_ids_to_check}},
			fields:{status:1,error:1,process_output:1}
		};
		NN.processRequest(req0,function(tmp) {
			if (!tmp.success) {
				job0.error='Problem finding process: '+tmp.error;
				callback({complete:true});
				return;
			}
			else {
				var docs=tmp.docs;
				var results_by_id={};
				docs.forEach(function(doc) {
					results_by_id[doc._id]=doc;
				});
				var done=true;
				job0.submitted_processes.forEach(function(PP) {
					if (PP.process_id in results_by_id) {
						var doc=results_by_id[PP.process_id];
						PP.status=doc.status;
						if (PP.status=='error') {
							job0.error='Processing has one or more errors (*).';
						}
						else if (PP.status!='finished') done=false;
					}
				});
				if (done) {
					callback({complete:true});
				}
				else {
					callback({complete:false});
				}
			}
		});
	}
	
	function make_random_id(numchars) {
		if (!numchars) numchars=10;
		var text = "";
		var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
		for( var i=0; i < numchars; i++ ) text += possible.charAt(Math.floor(Math.random() * possible.length));	
		return text;
	}
}