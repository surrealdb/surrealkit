-- The readings of one station at or above a floor.
SELECT VALUE reading FROM gauge
	WHERE station = $station AND reading >= $floor
	ORDER BY reading;
