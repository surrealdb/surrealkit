{
  LET $words = (__WORDS__);
  LET $readiness = __READINESS__;
  LET $near = IF $readiness = 'ready' { (__NEAR__) } ELSE { [] };
  { words: $words, near: $near, readiness: $readiness }
}
