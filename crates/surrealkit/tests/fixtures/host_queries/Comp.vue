<template>
  <p>{{ rows }}</p>
</template>

<script setup lang="ts">
const rows = await db.query("SELECT * FROM comment");
</script>
