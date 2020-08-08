DROP PROCEDURE IF EXISTS insertToTempTable;

delimiter //

CREATE PROCEDURE insertToTempTable ()
  BEGIN
      insert into sinhvien.temp select *from sinhvien.stagging;
  END//

delimiter ;
call insertToTempTable()
delimiter //

CREATE PROCEDURE truncateTable ()
  BEGIN

     TRUNCATE TABLE sinhvien.stagging ;
  END//

delimiter ;
SET SQL_SAFE_UPDATES = 0;
delete from sinhvien.temp where sinhvien.temp.stt like 'stt';

ALTER TABLE sinhvien.temp MODIFY masv varchar(250) AFTER ten;

delimiter //

CREATE PROCEDURE getAllDataconfig (in id_config int)
  BEGIN
      SELECT * FROM datacontrol.dataconfig 
      where datacontrol.dataconfig.id= id_config;
  END//

delimiter ;log
call getAllDataconfig(1);
delimiter //

CREATE PROCEDURE useDb (in db varchar(50) 
  BEGIN
     use db;
  END//

delimiter ;