exec GPROD1M_GCFR.GCFR_Register_System(1, 'Economic', '',  'Economic');
call GPROD1P_UT.GCFR_UT_Register_Stream(1, 1, 'Economic stream', cast('2019-01-01' as date));

delete from GPROD1t_GCFR.PARAMETERS where PARAMETER_ID in (11, 7, 10);

insert into GPROD1t_GCFR.PARAMETERS values (11, 'LRD T DB', 'GPROD1T_SRCI');
insert into GPROD1t_GCFR.PARAMETERS values (7, 'INPUT V DB', 'GPROD1V_INP');
insert into GPROD1t_GCFR.PARAMETERS values (10, 'BASE T DB', 'GPROD1T_BASE');

