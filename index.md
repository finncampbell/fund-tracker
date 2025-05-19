---
layout: default
title: "Fund Tracker Dashboard"
permalink: /
---

## Fund Tracker Dashboard

<div class="ft-nav">
  <button data-filter="" class="active">All</button>
  <button data-filter="Ventures">Ventures</button>
  <button data-filter="Capital">Capital</button>
  <button data-filter="Equity">Equity</button>
  <button data-filter="Advisors">Advisors</button>
  <button data-filter="Partners">Partners</button>
  <button data-filter="SIC">SIC Codes</button>
  <button data-filter="fund">Fund</button>
  <button data-filter="gp">GP</button>
  <button data-filter="lp">LP</button>
  <button data-filter="llp">LLP</button>
  <button data-filter="investments">Investments</button>
</div>

<table id="companies" class="display" style="width:100%">
  <thead>
    <tr>
      <th>Company Name</th>
      <th>Company Number</th>
      <th>Incorporation Date</th>
      <th>Status</th>
      <th>Source</th>
      <th>Time Discovered</th>
      <th>Date Downloaded</th>
    </tr>
  </thead>
  <tbody></tbody>
</table>

<script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js"></script>
<script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
<script>
  let table;

  // Load CSV and initialize DataTable
  Papa.parse("{{ '/assets/data/master_companies.csv' | relative_url }}", {
    download: true,
    header: true,
    complete: results => {
      table = $('#companies').DataTable({
        data: results.data,
        columns: [
          { data: 'Company Name' },
          { data: 'Company Number' },
          { data: 'Incorporation Date' },
          { data: 'Status' },
          { data: 'Source' },
          { data: 'Time Discovered' },
          { data: 'Date Downloaded' }
        ],
        order: [[2, 'desc']],      // newest incorporations first
        pageLength: 25,
        responsive: true
      });
    },
    error: err => console.error('CSV load error:', err)
  });

  // Wire up the filter buttons
  document.querySelectorAll('.ft-nav button').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelector('.ft-nav button.active').classList.remove('active');
      btn.classList.add('active');
      const key = btn.dataset.filter;

      if (!key) {
        // clear only the Source column filter
        table.column(4).search('').draw();
      } else {
        table.column(4).search(key, true, false).draw();
      }
    });
  });
</script>
