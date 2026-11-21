RETURN IF $tenant = NONE {
  { status: 'denied' }
} ELSE {
  LET $shelf = (SELECT id, version FROM ONLY type::record('shelf', $tenant));
  IF $shelf = NONE { THROW 'No shelf' } ELSE {
    { status: 'ready', version: $shelf.version, data: (__BODY__) }
  }
};
