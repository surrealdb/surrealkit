<template>
  <Query q="SELECT * FROM person WHERE age &gt; 18" />
  <Query :q="`SELECT * FROM post WHERE author = ${author}`" />
  <LiveQuery v-bind:q="'SELECT * FROM tag'" />
  <Other q="SELECT * FROM other" />
  <Query :q="sql" />
  <Query q="SELECT * FROM note WHERE body = 'a > b'" />
  <!-- <Query q="SELECT * FROM commented" /> -->
</template>

<script setup lang="ts">
const comments = useQuery("SELECT * FROM comment");
</script>
