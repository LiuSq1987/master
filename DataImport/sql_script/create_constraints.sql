CREATE INDEX idx_meteorological_station_station_no
	ON meteorological_station
	USING btree
	(station_no);
	
CREATE INDEX idx_meteorological_element_data_station_no_record_time
	ON meteorological_element_data
	USING btree
	(station_no, record_time);
	
CREATE INDEX idx_meteorological_forecast_data_city_record_time
	ON meteorological_forecast_data
	USING btree
	(city, record_time);
	
CREATE INDEX idx_water_quality_data_station_no_record_time
	ON water_quality_data
	USING btree
	(station_no, record_time);

CREATE UNIQUE INDEX idx_unq_water_station_station_no
	ON water_station
	USING btree
	(station_no);