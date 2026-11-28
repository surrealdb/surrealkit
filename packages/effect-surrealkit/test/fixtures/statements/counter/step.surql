LET $start = $boundStart;
LET $next = $start + 1;
IF $next > 100 { THROW 'Too large' };
FOR $i IN [1, 2] { LET $local = $i; };
RETURN $next * 2;
