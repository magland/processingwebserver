
var common=require('../processingnodeclient/src/common').common;
var wisdmconfig=require('./wisdmconfig').wisdmconfig;
var fs=require('fs');
var DATABASE=require('../processingnodeclient/src/databasemanager').DATABASE;

function temporaryfileserver(request,callback) {
	var user_id=(request.auth_info||{}).user_id;
	var permissions=(request.auth_info||{}).permissions||{};
	var command=request.command||'';
	
	if (!user_id) {
		callback({success:false,error:'user_id is_empty'});
		return;
	}
	
	var DB=DATABASE('temporaryfileserver');
	
	if (command=='createFolder') {
		if (user_id!=(request.owner||'')) {
			callback({success:false,error:'owner does not match user_id'});
			return;
		}
		
		
		var folder_id=common.make_random_id(20);
		var timestamp=(new Date()).getTime()
		DB.setCollection('folders');
		DB.insert({_id:folder_id,folder_id:folder_id,owner:request.owner,name:request.name,created:timestamp},function(err) {
			if (err) {callback({success:false,error:'Problem inserting folder: '+err}); return;}
			callback({success:true,folder_id:folder_id});
		});
		
		return;
	}
	else if (command=='getFolderList') {
		DB.setCollection('folders');
		DB.find({owner:request.owner},{},function(err,docs) {
			if (err) {callback({success:false,error:'Problem finding folders: '+err}); return;}
			callback({success:true,folders:docs});
		});
	}
	else if (command=='getFileList') {
		DB.setCollection('files');
		DB.find({folder_id:request.folder_id},{},function(err,docs) {
			if (err) {callback({success:false,error:'Problem finding files: '+err}); return;}
			callback({success:true,files:docs});
		});
	}
	else if (command=='uploadFile') {
		DB.setCollection('folders');
		DB.find({_id:request.folder_id},{owner:1},function(err,docs) {
			if (err) {
				callback({success:false,error:'Problem finding folder: '+err});
				return;
			}
			if (docs.length===0) {
				callback({success:false,error:'Unable to find folder: '+request.folder_id});
				return;
			}
			var folder0=docs[0];
			if (folder0.owner!=user_id) {
				callback({success:false,error:'You are not authorized to upload to this folder.'});
				return;
			}
			var file_id=request.folder_id+'::'+request.file_name;
			var data=new Buffer(request.file_data_base64,'base64');
			var checksum=compute_sha1(data);
			var suf=common.get_file_suffix(request.file_name);
			var path=wisdmconfig.temporaryfileserver.data_file_path;
			common.mkdir(path,function() {
			common.mkdir(path+'/'+checksum,function() {
				fs.writeFile(path+'/'+checksum+'/'+request.file_name,data,function(err) {
					if (err) {
						callback({success:false,error:'Problem writing file: '+err});
						return;
					}
					var timestamp=(new Date()).getTime();
					DB.setCollection('files');
					DB.save({
						_id:file_id,
						file_name:request.file_name,
						folder_id:request.folder_id,
						checksum:checksum,
						suffix:suf,
						created:timestamp,
						accessed:timestamp,
						size:0 //fix this
					},function(err) {
						if (err) {
							callback({success:false,error:'Problem saving file: '+err});
							return;
						}
						callback({success:true});
					});
				});
			});});
		});
	}
	else {
		callback({success:false,error:'Unrecognized command in temporaryfileserver: '+command});
	}
	
	/*
	function open_database(params,callback) {
		var db=new mongo.Db('temporaryfileserver', new mongo.Server('localhost',params.port||27017, {}), {safe:true});
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
	
	function compute_sha1(data) {
		var crypto=require('crypto');
		var ret=crypto.createHash('sha1');
		ret.update(data);
		return ret.digest('hex');
	}
}
	
exports.temporaryfileserver=temporaryfileserver;