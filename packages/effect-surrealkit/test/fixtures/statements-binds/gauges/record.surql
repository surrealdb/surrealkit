-- Records a reading, scaled by the schema function.
CREATE gauge SET station = $station, reading = fn::scaled($raw), site = $site, tags = $tags;
