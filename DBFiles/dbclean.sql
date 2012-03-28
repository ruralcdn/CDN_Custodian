use ruralcdn

delete from dbsync;
delete from status;
delete from uploadrequest;
delete from uploadeditem;
delete from dtnrequest;
delete from status;
delete from downloadrequest;
delete from userupload;
delete from localdata;
delete from synctable;
delete from userlocation;
delete from userdaemonloc;


update synctable set updated_till=0;
