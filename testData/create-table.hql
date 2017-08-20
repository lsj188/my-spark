--drop table mytest.t_dim_regio;
CREATE TABLE mytest.t_dim_regio
(
  regio_id               INTEGER                      , 
  regio_name             CHARACTER VARYING(50)        
  update_time            BIGINT      
);

--drop table mytest.t_dim_date;
CREATE TABLE mytest.t_dim_date
(
  date                   BIGINT      , 
  year                   int         ,
  month                  int         ,
  day                    int         ,
  week                   int         , 
  quarter                int         
);

--drop table mytest.t_iete_flow;
CREATE TABLE mytest.t_iete_flow
(
  time_id                BIGINT                       , 
  mdn                    CHARACTER VARYING(20)        , 
  imsi                   CHARACTER VARYING(50)        , 
  regio_id               INTEGER                      , 
  cell_flag	             INTEGER,    	
  flow                   NUMERIC(28,4)                    
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (regio_id)
PARTITION BY RANGE(tim_id) 
          (
          PARTITION p_20150801 START (20150801) END (20150901),
          PARTITION p_20150901 START (20150901) END (20151001)
          )
;

--drop table mytest.t_iete_usrlove_subclass_mstype_m;
CREATE TABLE mytest.t_iete_usrlove_subclass_mstype_m
(
  tim_id                 BIGINT                       , 
  hour_region            INTEGER                      , 
  mdn                    CHARACTER VARYING(20)        , 
  imsi                   CHARACTER VARYING(50)        , 
  regio_id               INTEGER                      , 
  regio_name             CHARACTER VARYING(50)        , 
  gsm_flow_flag	         INTEGER,    	
  td_flow_flag	         INTEGER,    		
  lte_flow_flag	         INTEGER,
  flow                   NUMERIC(28,4)                    

)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (regio_id)
PARTITION BY RANGE(tim_id) 
          (
          PARTITION p_20150801 START (20150801) END (20150901),
          PARTITION p_20150901 START (20150901) END (20151001)
          )
;
ALTER TABLE mytest.t_iete_usrlove_subclass_mstype_m
  OWNER TO gpadmin;
COMMENT ON TABLE mytest.t_iete_usrlove_subclass_mstype_m
  IS '用户业务偏好-即席查询-业务小类-月';
COMMENT ON COLUMN mytest.t_iete_usrlove_subclass_mstype_m.tim_id             IS '时间        ';
COMMENT ON COLUMN mytest.t_iete_usrlove_subclass_mstype_m.hour_region        IS '时段区     ';
COMMENT ON COLUMN mytest.t_iete_usrlove_subclass_mstype_m.mdn                IS '用户号码    ';
COMMENT ON COLUMN mytest.t_iete_usrlove_subclass_mstype_m.imsi               is '用户IMSI    ';
COMMENT ON COLUMN mytest.t_iete_usrlove_subclass_mstype_m.regio_id           is '地市ID      ';
COMMENT ON COLUMN mytest.t_iete_usrlove_subclass_mstype_m.regio_name         is '地市        ';
COMMENT ON COLUMN mytest.t_iete_usrlove_subclass_mstype_m.flow               is '流量        ';
COMMENT ON COLUMN mytest.t_iete_usrlove_subclass_mstype_m.gsm_flow_flag      is '2g流量档标记	1、0-100，2、100-200，3、200-300，4、300以上  ';
COMMENT ON COLUMN mytest.t_iete_usrlove_subclass_mstype_m.td_flow_flag       is 'td流量档标记';
COMMENT ON COLUMN mytest.t_iete_usrlove_subclass_mstype_m.lte_flow_flag      is 'lte流量档标记 ';

--地市周内流量前10用户