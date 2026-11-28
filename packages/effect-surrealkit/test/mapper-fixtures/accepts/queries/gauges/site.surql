-- One site, by its string key.
SELECT VALUE label FROM type::record('site', $siteKey);
