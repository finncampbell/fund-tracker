$(document).ready(function() {
  // Cache‐busted URL so the browser always pulls fresh data
  const url = `assets/data/relevant_companies.csv?v=${Date.now()}`;
  const fundEntitiesRE = /\bFund\b|\bG[\.\-\s]?P\b|\bL[\.\-\s]?L[\.\-\s]?P\b|\bL[\.\-\s]?P\b/i;

  Papa.parse(url, {
    download: true,
    header: true,
    complete(results) {
      const raw = results.data.filter(r => r['Company Number']);

      let directorsMap = {};
      fetch('assets/data/directors.json')
        .then(r => r.json())
        .then(json => {
          directorsMap = Object.fromEntries(
            Object.entries(json).map(([k, v]) => [k.trim(), v])
          );
          initTables();
        })
        .catch(err => {
          console.error('Failed to load directors.json', err);
          initTables();
        });

      function initTables() {
        const data = raw.map(r => {
          const num = r['Company Number'].trim();
          return { ...r, 'Company Number': num, Directors: directorsMap[num] || [] };
        });
        // ... rest of DataTables initialization as before ...
      }
    }
  });
  // ... remainder of your dashboard.js (backfill controls, polling) ...
});
