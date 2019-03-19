-------------------------------------------------------------------------------------------------------------
-- meteorological table index
-------------------------------------------------------------------------------------------------------------
CREATE INDEX idx_tmp_meteorological_element_data_station_no_record_time
	ON tmp_meteorological_element_data
	USING btree
	(station_no, record_time);

CREATE INDEX idx_tmp_meteorological_forecast_data_city_record_time
	ON tmp_meteorological_forecast_data
	USING btree
	(city, record_time);

-------------------------------------------------------------------------------------------------------------
-- water quality table index
-------------------------------------------------------------------------------------------------------------
CREATE INDEX idx_tmp_water_quality_data_station_no_record_time
	ON tmp_water_quality_data
	USING btree
	(station_no, record_time);

CREATE INDEX idx_tmp_water_station_station_no
	ON tmp_water_station
	USING btree
	(station_no);