
CREATE TABLE "ObsPlan" (
	t_planning DOUBLE PRECISION NOT NULL, 
	target_name CHAR, 
	obs_id CHAR NOT NULL, 
	obs_collection CHAR, 
	s_ra DOUBLE PRECISION, 
	s_dec DOUBLE PRECISION, 
	s_fov DOUBLE PRECISION, 
	s_region CHAR, 
	s_resolution DOUBLE PRECISION, 
	t_min DOUBLE PRECISION NOT NULL, 
	t_max DOUBLE PRECISION NOT NULL, 
	t_exptime DOUBLE PRECISION NOT NULL, 
	t_resolution DOUBLE PRECISION, 
	em_min DOUBLE PRECISION, 
	em_max DOUBLE PRECISION, 
	em_res_power DOUBLE PRECISION, 
	o_ucd CHAR, 
	pol_states CHAR, 
	pol_xel INTEGER, 
	facility_name CHAR NOT NULL, 
	instrument_name CHAR, 
	t_plan_exptime DOUBLE PRECISION, 
	category CHAR NOT NULL, 
	priority INTEGER NOT NULL, 
	execution_status CHAR NOT NULL, 
	tracking_type CHAR NOT NULL, 
	rubin_rot_sky_pos FLOAT, 
	rubin_nexp INTEGER
)

;
COMMENT ON TABLE "ObsPlan" IS 'Metadata in the ObsLocTap relational realization of the IVOA ObsLocTap data model';
COMMENT ON COLUMN "ObsPlan".t_planning IS 'Time in MJD when this observation has been added or modified into the planning log';
COMMENT ON COLUMN "ObsPlan".target_name IS 'Astronomical object observed, if any';
COMMENT ON COLUMN "ObsPlan".obs_id IS 'Observation ID';
COMMENT ON COLUMN "ObsPlan".obs_collection IS 'Name of the data collection';
COMMENT ON COLUMN "ObsPlan".s_ra IS 'Central right ascension, ICRS';
COMMENT ON COLUMN "ObsPlan".s_dec IS 'Central declination, ICRS';
COMMENT ON COLUMN "ObsPlan".s_fov IS 'Diameter (bounds) of the covered region';
COMMENT ON COLUMN "ObsPlan".s_region IS 'Sky region covered by the data product (expressed in ICRS frame)';
COMMENT ON COLUMN "ObsPlan".s_resolution IS 'Spatial resolution of data as FWHM';
COMMENT ON COLUMN "ObsPlan".t_min IS 'Start time in MJD';
COMMENT ON COLUMN "ObsPlan".t_max IS 'Stop time in MJD';
COMMENT ON COLUMN "ObsPlan".t_exptime IS 'Total exposure time';
COMMENT ON COLUMN "ObsPlan".t_resolution IS 'Temporal resolution FWHM';
COMMENT ON COLUMN "ObsPlan".em_min IS 'Start in spectral coordinates';
COMMENT ON COLUMN "ObsPlan".em_max IS 'Stop in spectral coordinates';
COMMENT ON COLUMN "ObsPlan".em_res_power IS 'Spectral resolving power';
COMMENT ON COLUMN "ObsPlan".o_ucd IS 'UCD of observable (e.g., phot.flux.density, phot.count, etc.)';
COMMENT ON COLUMN "ObsPlan".pol_states IS 'List of polarization states or NULL if not applicable';
COMMENT ON COLUMN "ObsPlan".pol_xel IS 'Number of polarization samples';
COMMENT ON COLUMN "ObsPlan".facility_name IS 'Name of the facility used for this observation';
COMMENT ON COLUMN "ObsPlan".instrument_name IS 'Name of the instrument used for this observation';
COMMENT ON COLUMN "ObsPlan".t_plan_exptime IS 'Planned or scheduled exposure time';
COMMENT ON COLUMN "ObsPlan".category IS 'Observation category. One of the following values\\u0003A Fixed, Coordinated, Window, Other';
COMMENT ON COLUMN "ObsPlan".priority IS 'Priority level { 0, 1, 2}';
COMMENT ON COLUMN "ObsPlan".execution_status IS 'One of the following values\\u0003A Planned, Scheduled, Unscheduled, Performed, Aborted';
COMMENT ON COLUMN "ObsPlan".tracking_type IS 'One of the following values\\u0003A Sidereal, Solar-system-object-tracking, Fixed-az-el-transit';
COMMENT ON COLUMN "ObsPlan".rubin_rot_sky_pos IS 'From scheduler rotation angle Not in the IVOA ObsPlan table';
COMMENT ON COLUMN "ObsPlan".rubin_nexp IS 'number of exposures to take usually 1 or 2 Not in the IVOA ObsPlan table';
