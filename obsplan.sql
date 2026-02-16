
CREATE TABLE obsplan (
	t_planning DOUBLE PRECISION NOT NULL,
	target_name VARCHAR,
	obs_id VARCHAR NOT NULL,
	obs_collection VARCHAR,
	s_ra DOUBLE PRECISION,
	s_dec DOUBLE PRECISION,
	s_fov DOUBLE PRECISION,
	s_region VARCHAR,
	s_resolution DOUBLE PRECISION,
	t_min DOUBLE PRECISION NOT NULL,
	t_max DOUBLE PRECISION NOT NULL,
	t_exptime DOUBLE PRECISION NOT NULL,
	t_resolution DOUBLE PRECISION,
	em_min DOUBLE PRECISION,
	em_max DOUBLE PRECISION,
	em_res_power DOUBLE PRECISION,
	o_ucd VARCHAR,
	pol_states VARCHAR,
	pol_xel INTEGER,
	facility_name VARCHAR NOT NULL,
	instrument_name VARCHAR,
	t_plan_exptime DOUBLE PRECISION,
	category VARCHAR NOT NULL,
	priority INTEGER NOT NULL,
	execution_status VARCHAR NOT NULL,
	tracking_type VARCHAR NOT NULL,
	rubin_rot_sky_pos FLOAT,
	rubin_nexp INTEGER
)

;
COMMENT ON TABLE obsplan IS 'Metadata in the ObsLocTap relational realization of the IVOA ObsLocTap data model';
COMMENT ON COLUMN obsplan.t_planning IS 'Time in MJD when this observation has been added or modified into the planning log';
COMMENT ON COLUMN obsplan.target_name IS 'Astronomical object observed, if any';
COMMENT ON COLUMN obsplan.obs_id IS 'Observation ID';
COMMENT ON COLUMN obsplan.obs_collection IS 'Name of the data collection';
COMMENT ON COLUMN obsplan.s_ra IS 'Central right ascension, ICRS';
COMMENT ON COLUMN obsplan.s_dec IS 'Central declination, ICRS';
COMMENT ON COLUMN obsplan.s_fov IS 'Diameter (bounds) of the covered region';
COMMENT ON COLUMN obsplan.s_region IS 'Sky region covered by the data product (expressed in ICRS frame)';
COMMENT ON COLUMN obsplan.s_resolution IS 'Spatial resolution of data as FWHM';
COMMENT ON COLUMN obsplan.t_min IS 'Start time in MJD';
COMMENT ON COLUMN obsplan.t_max IS 'Stop time in MJD';
COMMENT ON COLUMN obsplan.t_exptime IS 'Total exposure time';
COMMENT ON COLUMN obsplan.t_resolution IS 'Temporal resolution FWHM';
COMMENT ON COLUMN obsplan.em_min IS 'Start in spectral coordinates';
COMMENT ON COLUMN obsplan.em_max IS 'Stop in spectral coordinates';
COMMENT ON COLUMN obsplan.em_res_power IS 'Spectral resolving power';
COMMENT ON COLUMN obsplan.o_ucd IS 'UCD of observable (e.g., phot.flux.density, phot.count, etc.)';
COMMENT ON COLUMN obsplan.pol_states IS 'List of polarization states or NULL if not applicable';
COMMENT ON COLUMN obsplan.pol_xel IS 'Number of polarization samples';
COMMENT ON COLUMN obsplan.facility_name IS 'Name of the facility used for this observation';
COMMENT ON COLUMN obsplan.instrument_name IS 'Name of the instrument used for this observation';
COMMENT ON COLUMN obsplan.t_plan_exptime IS 'Planned or scheduled exposure time';
COMMENT ON COLUMN obsplan.category IS 'Observation category. One of the following values\\u0003A Fixed, Coordinated, Window, Other';
COMMENT ON COLUMN obsplan.priority IS 'Priority level { 0, 1, 2}';
COMMENT ON COLUMN obsplan.execution_status IS 'One of the following values\\u0003A Planned, Scheduled, Unscheduled, Performed, Aborted';
COMMENT ON COLUMN obsplan.tracking_type IS 'One of the following values\\u0003A Sidereal, Solar-system-object-tracking, Fixed-az-el-transit';
COMMENT ON COLUMN obsplan.rubin_rot_sky_pos IS 'From scheduler rotation angle Not in the IVOA obsplan table';
COMMENT ON COLUMN obsplan.rubin_nexp IS 'number of exposures to take usually 1 or 2 Not in the IVOA obsplan table';
