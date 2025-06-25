// docs/assets/js/fca-dashboard.js

$(document).ready(function(){
  // Variables to hold loaded data
  let firmsData, namesData, arsData, cfData, indivData, personsData;

  // Load all JSON slices in parallel from docs/fca-dashboard/data/
  $.when(
    $.getJSON('fca-dashboard/data/fca_firms.json', d => { firmsData = d; }),
    $.getJSON('fca-dashboard/data/fca_names.json', d => { namesData = d; }),
    $.getJSON('fca-dashboard/data/fca_ars.json', d => { arsData = d; }),
    $.getJSON('fca-dashboard/data/fca_cf.json', d => { cfData = d; }),
    $.getJSON('fca-dashboard/data/fca_individuals_by_firm.json', d => { indivData = d; }),
    $.getJSON('fca-dashboard/data/fca_persons.json', d => { personsData = d; })
  ).then(initDashboard);

  // Tab switching logic
  $('.tabs button').click(function(){
    $('.tabs button').removeClass('active');
    $(this).addClass('active');
    $('.tab-content').removeClass('active');
    $('#' + $(this).data('tab')).addClass('active');
  });

  function initDashboard(){
    initFirmsTable();
    initIndividualsTable();
    initMatchesTable();
  }

  function initFirmsTable(){
    const tbl = $('#firms-table').DataTable({
      data: firmsData,
      columns: [
        {
          className: 'dt-control',
          orderable: false,
          data: null,
          defaultContent: ''
        },
        { data: 'frn',               title: 'FRN' },
        { data: 'organisation_name', title: 'Organisation Name' },
        { data: 'status',            title: 'Status' },
        { data: 'business_type',     title: 'Business Type' },
        { data: 'companies_house_number', title: 'CH#' },
        { data: d => (namesData[d.frn] || []).length,      title: '#Names' },
        { data: d => (arsData[d.frn]  || []).length,      title: '#ARs' },
        { data: d => (cfData[d.frn]   || []).filter(c=>c.section==='Current').length, title: '#CF Curr' },
        { data: d => (cfData[d.frn]   || []).filter(c=>c.section==='Previous').length, title: '#CF Prev' },
        { data: d => (indivData[d.frn]|| []).length,      title: '#Inds' }
      ],
      order: [[1,'asc']]
    });

    // Expand/collapse child‑row on click
    $('#firms-table tbody').on('click', 'td.dt-control', function(){
      const tr = $(this).closest('tr'),
            row = tbl.row(tr);

      if (row.child.isShown()) {
        row.child.hide();
      } else {
        row.child(renderFirmDetails(row.data())).show();
      }
    });
  }

  function renderFirmDetails(d){
    function renderList(title, arr){
      if(!arr || arr.length===0) return '';
      return `<strong>${title}:</strong><ul>` +
             arr.map(n=>`<li>${n||''}</li>`).join('') +
             `</ul>`;
    }

    function renderTable(title, cols, data){
      if(!data || data.length===0) return '';
      const header = `<thead><tr>${
        cols.map(c=>`<th>${c}</th>`).join('')
      }</tr></thead>`;

      const body = `<tbody>${
        data.map(r=>`<tr>${
          cols.map(c=>`<td>${r[c] != null ? r[c] : ''}</td>`).join('')
        }</tr>`).join('')
      }</tbody>`;

      return `<strong>${title}:</strong>` +
             `<table class="child-table">${header}${body}</table>`;
    }

    return `<div class="child-rows">` +
           renderList('Trading Names', namesData[d.frn]) +
           renderTable('Appointed Reps', ['Name','Status'], arsData[d.frn]) +
           renderTable('Controlled Functions',
                       ['section','controlled_function','Individual Name','Effective Date'],
                       cfData[d.frn]) +
           renderTable('Firm Individuals', ['IRN','Name','Status'], indivData[d.frn]) +
           `</div>`;
  }

  function initIndividualsTable(){
    const allPersons = Object.values(personsData || {});
    $('#individuals-table').DataTable({
      data: allPersons,
      columns: [
        { data: 'irn',           title: 'IRN' },
        { data: 'name',          title: 'Name' },
        { data: 'status',        title: 'Status' },
        { data: 'date_of_birth', title: 'DoB' },
        {
          data: d => Object.entries(indivData || {})
                         .filter(([frn, arr]) => arr.some(e=>e.IRN===d.irn))
                         .map(([frn])=>frn).join(', '),
          title: 'Firms'
        },
        {
          data: d => {
            let count = 0;
            Object.values(cfData || {}).forEach(arr =>
              arr.forEach(c => { if(c['Individual Name']===d.name) count++; })
            );
            return count;
          },
          title: '#CF Records'
        }
      ],
      order: [[1,'asc']]
    });
  }

  function initMatchesTable(){
    $('#matches-table').DataTable({
      data: [], // populate when match data available
      columns: [
        { title: 'CH Entity' },
        { title: 'Reg Date' },
        { title: 'Match Type' },
        { title: 'Matched To' },
        { title: 'Link' }
      ]
    });
  }
});
