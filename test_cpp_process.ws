/* jobKey: general */

include('wisdmws:/wisdm.ws');

function run() {
	disp('testing cpp process');
	
	BEGIN_PROCESS cpp [nii X]=test_cpp_process()
	printf("hello, cpp!\n");
	
	X.allocate(20,20,1);
	for (int y=0; y<20; y++)
	for (int x=0; x<20; x++) {
		X.setValue(x+y,x,y);
	}
	
	END_PROCESS
	
	view(X,{title:'X'});
	
	BEGIN_PROCESS cpp [nii Y]=test_cpp_2(nii X)
	Y=X;
	Y.setValue(50,10,10);
	END_PROCESS
	
	view(Y,{title:'Y'});
	
	BEGIN_PROCESS octave [mda Z]=test_cpp_3(nii Y)
	Z=Y.image;
	Z(1:end,4)=10;
	END_PROCESS
	
	view(Z,{title:'Z'});
	
}
