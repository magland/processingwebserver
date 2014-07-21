//rename this file to wisdmconfig.js and modify

var wisdmconfig={};

wisdmconfig.processingwebserver={
	listen_port:8001,
	www_path:'/home/magland/wisdm/www/processingwebserver',
	wisdmserver_url:'http://localhost:8000'
};

wisdmconfig.processingnodeserver={
	listen_port:8082
};

wisdmconfig.temporaryfileserver={
	data_file_path:'/home/magland/wisdm/www/processingwebserver/temporaryfileserver'
};

exports.wisdmconfig=wisdmconfig;
