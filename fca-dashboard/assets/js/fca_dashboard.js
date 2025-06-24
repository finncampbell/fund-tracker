$(document).ready(function(){
  // 1. load JSON data
  let firms, names, ars, cf, indiv, persons;
  $.when(
    $.getJSON('data/fca_firms.json', d=>firms=d),
    $.getJSON('data/fca_names.json', d=>names=d),
    $.getJSON('data/fca_ars.json', d=>ars=d),
    $.getJSON('data/fca_cf.json', d=>cf=d),
    $.getJSON('data/fca_individuals_by_firm.json', d=>indiv=d),
    $.getJSON('data/fca_persons.json', d=>persons=d)
  ).then(initTables);

  function initTables(){
    // 2. FIRMS DataTable
    const firmTable = $('#firms-table').DataTable({
      data: firms,
      columns: [
        { className: 'dt-control', orderable: false, data: null, defaultContent: '' },
        { data: 'frn', title: 'FRN' },
        { data: 'organisation_name', title: 'Name' },
        // … other top‑level columns …
      ],
      order: [[1,'asc']]
    });
    // 3. toggle child row
    $('#firms-table tbody').on('click', 'td.dt-control', function(){
      let tr = $(this).closest('tr'),
          row = firmTable.row(tr);
      if (row.child.isShown()) row.child.hide();
      else row.child(renderFirmDetails(row.data())).show();
    });

    // 4. similar for individuals & matches…
  }

  function renderFirmDetails(d){
    // Build a small HTML table or list for:
    // - trading_names (names[d.frn])
    // - appointed_reps (ars[d.frn])
    // - controlled_functions (cf[d.frn])
    // - firm_individuals (indiv[d.frn])
    // — and optionally a 2nd‑level child using persons[irn].
    return `<div class="child-rows">
      ${renderList('Trading Names', names[d.frn])}
      ${renderTable('Appointed Reps', ['Name','Status'], ars[d.frn])}
      ${renderTable('Controlled Functions', ['section','controlled_function','Individual Name','Effective Date'], cf[d.frn])}
      ${renderTable('Firm Individuals', ['IRN','Name','Status'], indiv[d.frn])}
    </div>`;
  }
});
