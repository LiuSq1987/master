-------------------------------------------------------------------------------------------------------------
-- meteorological data
-------------------------------------------------------------------------------------------------------------
CREATE TABLE meteorological_station
(
	id						serial PRIMARY KEY,
	station_no				CHAR(5) NOT NULL, --站号
	station_name			VARCHAR(64),--站名
	longitude				DOUBLE PRECISION NOT NULL, --经度
	latitude				DOUBLE PRECISION NOT NULL --纬度
);

CREATE TABLE meteorological_element_data
(
	id						serial PRIMARY KEY,
	record_time				TIMESTAMP NOT NULL, --记录时间
	station_no				CHAR(5) NOT NULL, --站号
	station_name			VARCHAR(64),--站名
	longitude				DOUBLE PRECISION NOT NULL, --经度
	latitude				DOUBLE PRECISION NOT NULL, --纬度
	precipitation			DOUBLE PRECISION, --1小时降水量(mm)
	temperature				DOUBLE PRECISION, --当前气温(℃)
	wind_direction			INTEGER, --瞬时风向(度)
	wind_speed				DOUBLE PRECISION, --瞬时风速(m/s)
	evaporation				DOUBLE PRECISION --蒸发量(mm)
);

CREATE TABLE meteorological_forecast_data
(
	id						serial PRIMARY KEY,
	record_time				TIMESTAMP NOT NULL, --记录时间
	city					VARCHAR(64) NOT NULL, --城市
	high_temperature_24		DOUBLE PRECISION, --高温24
	low_temperature_24		DOUBLE PRECISION, --低温24
	weather_12				VARCHAR(16),      --天气12
	weather_24				VARCHAR(16),      --天气24
	wind_direction_12		VARCHAR(16),      --风向12
	wind_speed_12           VARCHAR(16),      --风速12
	wind_direction_24		VARCHAR(16),      --风向24
	wind_speed_24           VARCHAR(16),      --风速24
	high_temperature_48		DOUBLE PRECISION, --高温48
	low_temperature_48		DOUBLE PRECISION, --低温48
	weather_36				VARCHAR(16),      --天气36
	weather_48				VARCHAR(16),      --天气48
	wind_direction_36		VARCHAR(16),      --风向36
	wind_speed_36			VARCHAR(16),      --风速36
	wind_direction_48		VARCHAR(16),      --风向48
	wind_speed_48			VARCHAR(16),      --风速48
	high_temperature_72		DOUBLE PRECISION, --高温72
	low_temperature_72		DOUBLE PRECISION, --低温72
	weather_60				VARCHAR(16),      --天气60
	weather_72				VARCHAR(16),      --天气72
	wind_direction_60		VARCHAR(16),      --风向60
	wind_speed_60			VARCHAR(16),      --风速60
	wind_direction_72		VARCHAR(16),      --风向72
	wind_speed_72			VARCHAR(16)       --风速72
);



-------------------------------------------------------------------------------------------------------------
-- water quality data
-------------------------------------------------------------------------------------------------------------
CREATE TABLE water_station
(
	id                    serial PRIMARY KEY,
	station_no            CHAR(14) NOT NULL,           --监测站代码
	station_name          VARCHAR(64),           --监测站名称
	drainage_code         VARCHAR(64),           --流域代码
	area_code             VARCHAR(64) NOT NULL,           --地区代码
	address               VARCHAR(256),          --地址
	station_type          CHAR(1),               --监测站类型
	monitor_level         CHAR(2),               --监控级别
	longitude             DOUBLE PRECISION NOT NULL,      --经度
	latitude              DOUBLE PRECISION NOT NULL       --纬度
);

CREATE TABLE water_quality_data
(
	id                    serial PRIMARY KEY,
	station_no            CHAR(14) NOT NULL, --监测站代码
	station_name          VARCHAR(64), --监测站名称
	record_time           TIMESTAMP NOT NULL,
	f01                   DOUBLE PRECISION, --温度
	f02                   DOUBLE PRECISION, --湿度
	f03                   DOUBLE PRECISION, --电压
	f04                   DOUBLE PRECISION, --水压1(进样压1)
	f05                   DOUBLE PRECISION, --水压2(源水压1)
	f06                   DOUBLE PRECISION, --水压3(进样压2)
	f07                   DOUBLE PRECISION, --气压
	f08                   DOUBLE PRECISION, --水压4(源水压2)
	f09                   DOUBLE PRECISION, --PH值
	f10                   DOUBLE PRECISION, --水温
	f11                   DOUBLE PRECISION, --溶解氧
	f12                   DOUBLE PRECISION, --浊度
	f13                   DOUBLE PRECISION, --电导率
	f14                   DOUBLE PRECISION, --高锰酸盐指数
	f15                   DOUBLE PRECISION, --氨氮
	f16                   DOUBLE PRECISION, --总磷
	f17                   DOUBLE PRECISION, --总氮
	f18                   DOUBLE PRECISION, --TOC
	f19                   DOUBLE PRECISION, --总酚（挥发酚）
	f20                   DOUBLE PRECISION, --粪大肠菌群
	f21                   DOUBLE PRECISION, --叶绿素
	f22                   DOUBLE PRECISION, --水位
	f23                   DOUBLE PRECISION, --流向
	f24                   DOUBLE PRECISION, --流速
	f25                   DOUBLE PRECISION, --流量
	f26                   DOUBLE PRECISION, --总氯
	f27                   DOUBLE PRECISION, --化学需氧量
	f28                   DOUBLE PRECISION, --五日生化需氧量
	f29                   DOUBLE PRECISION, --石油类
	f30                   DOUBLE PRECISION, --色度
	f31                   DOUBLE PRECISION, --悬浮物
	f32                   DOUBLE PRECISION, --有机氮
	f33                   DOUBLE PRECISION, --蓝绿藻（藻蓝蛋白）
	f34                   DOUBLE PRECISION, --氟化物
	f35                   DOUBLE PRECISION, --氯化物
	f36                   DOUBLE PRECISION, --氰化物
	f37                   DOUBLE PRECISION, --汞
	f38                   DOUBLE PRECISION, --铜
	f39                   DOUBLE PRECISION, --铅
	f40                   DOUBLE PRECISION, --铁
	f41                   DOUBLE PRECISION, --锌
	f42                   DOUBLE PRECISION, --锰
	f43                   DOUBLE PRECISION, --铬
	f44                   DOUBLE PRECISION, --镉
	f45                   DOUBLE PRECISION, --砷
	f46                   DOUBLE PRECISION, --硒
	f47                   DOUBLE PRECISION, --生物毒性1（发光菌）
	f48                   DOUBLE PRECISION, --生物毒性2（鱼法）
	f49                   DOUBLE PRECISION, --硫化物
	f50                   DOUBLE PRECISION, --硝态氮
	f51                   DOUBLE PRECISION, --黄色物质
	f52                   DOUBLE PRECISION, --三氮甲烷
	f53                   DOUBLE PRECISION, --三氯乙烯
	f54                   DOUBLE PRECISION, --四氮乙烯
	f55                   DOUBLE PRECISION, --二氯甲烷
	f56                   DOUBLE PRECISION, --1，2-二氯乙烷
	f57                   DOUBLE PRECISION, --苯
	f58                   DOUBLE PRECISION, --甲苯
	f59                   DOUBLE PRECISION, --乙苯
	f60                   DOUBLE PRECISION, --二甲苯
	f61                   DOUBLE PRECISION, --氯苯
	f62                   DOUBLE PRECISION, --1，2-二氯苯
	f63                   DOUBLE PRECISION, --1，4-二氯苯
	f64                   DOUBLE PRECISION, --异丙苯
	f65                   DOUBLE PRECISION, --苯乙烯
	f66                   DOUBLE PRECISION, --对、间二四苯
	f67                   DOUBLE PRECISION, --1，2-二氯丙烷
	f68                   DOUBLE PRECISION, --反式-1，2-二氯乙烯
	f69                   DOUBLE PRECISION, --顺式-1，2-二氯乙烯
	f70                   DOUBLE PRECISION, --镍
	f71                   DOUBLE PRECISION, --绿藻
	f72                   DOUBLE PRECISION, --硅甲藻
	f73                   DOUBLE PRECISION, --隐藻
	f74                   DOUBLE PRECISION, --经度(GPS经度)
	f75                   DOUBLE PRECISION, --纬度
	f76                   DOUBLE PRECISION, --风向
	f77                   DOUBLE PRECISION, --风速
	f78                   DOUBLE PRECISION, --正磷酸盐
	f79                   DOUBLE PRECISION, --总磷(湖/库)
	f80                   DOUBLE PRECISION, --生物毒性
	f81                   DOUBLE PRECISION, --氨氮核定值
	f82                   DOUBLE PRECISION  --出口压力
);