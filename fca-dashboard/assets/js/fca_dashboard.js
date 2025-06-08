// fca-dashboard/assets/js/fca_dashboard.js
$(document).ready(function() {
  // Tab switching
  $('.ft-btn').on('click', function() {
    $('.ft-btn').removeClass('active');
    $(this).addClass('active');
    const tab = $(this).data('tab');
    ['firms','individuals','matches'].forEach(t => {
      $('#' + t + '-container')[ t === tab ? 'show' : 'hide' ]();
    });
  });

  // Load and render each table
  $.getJSON('data/fca_firms.json', initFirmsTable);
  $.getJSON('data/fca_individuals.json', initIndividualsTable);
  $.getJSON('data/fca_matches.json', initMatchesTable);

  function initFirmsTable(data) {
    const table = $('#fca-firms').DataTable({
      data,
      columns: [
        { data: 'name' },
        { data: 'frn' },
        { data: 'status' },
        { data: 'type' },
        { data: d => (d.linked_people||[]).length }
      ]
    });
    // Expandable child rows
    $('#fca-firms tbody').on('click', 'tr', function() {
      const row = table.row(this);
      if (row.child.isShown()) {
        row.child.hide();
      } else {
        row.child(formatFirmDetails(row.data())).show();
      }
    });
  }

  function initIndividualsTable(data) {
    const table = $('#fca-individuals').DataTable({
      data,
      columns: [
        { data: 'name' },
        { data: d => (d.roles || []).map(r => r.controlled_function).join(', ') },
        { data: d => (d.linked_firms || []).join(', ') },
        { data: 'status' },
        { data: 'last_seen' }
      ]
    });
    $('#fca-individuals tbody').on('click', 'tr', function() {
      const row = table.row(this);
      if (row.child.isShown()) {
        row.child.hide();
      } else {
        row.child(formatIndividualDetails(row.data())).show();
      }
    });
  }

  function initMatchesTable(data) {
    const table = $('#ch-matches').DataTable({
      data,
      order: [[1, 'desc']],
      columns: [
        { data: 'company_name' },
        { data: 'incorporation_date' },
        { data: 'matched_fca_firm', defaultContent: '' },
        { data: 'matched_fca_person', defaultContent: '' },
        { data: 'match_confidence' }
      ]
    });
    $('#ch-matches tbody').on('click', 'tr', function() {
      const row = table.row(this);
      if (row.child.isShown()) {
        row.child.hide();
      } else {
        row.child(formatMatchDetails(row.data())).show();
      }
    });
  }

  // Child‑row formatters (customize as you like)
  function formatFirmDetails(d) {
    const perms = (d.permissions||[]).join(', ');
    return `<table class="child-table">
      <tr><th>Permissions</th><td>${perms}</td></tr>
      <tr><th>AR of</th><td>${d.principal_firm||'—'}</td></tr>
      <tr><th>Address</th><td>${d.address||'—'}</td></tr>
    </table>`;
  }

  function formatIndividualDetails(d) {
    let rows = (d.roles||[]).map(r =>
      `<tr>
         <th>${r.controlled_function}</th>
         <td>${r.firm_name} (${r.status})<br/>
             ${r.first_seen} → ${r.last_seen || 'now'}</td>
       </tr>`).join('');
    return `<table class="child-table">${rows}</table>`;
  }

  function formatMatchDetails(d) {
    return `<table class="child-table">
      <tr><th>Match Type</th><td>${d.match_type}</td></tr>
      <tr><th>Company No</th><td>${d.company_number}</td></tr>
      <tr><th>Role Link</th><td>${d.matched_fca_person||'—'}</td></tr>
    </table>`;
  }
});

