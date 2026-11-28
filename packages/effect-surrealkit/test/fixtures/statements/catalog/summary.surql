{
  LET $rows = (SELECT id, title, tags FROM listing WHERE shelf = $shelf.id LIMIT 10);
  __COUNTS__
  { total: $total, titles: array::map($rows, |$row| __TITLE__) }
}
