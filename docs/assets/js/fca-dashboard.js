$(document).ready(function(){
  let firmsData, namesData, arsData, cfData, indivData, personsData;

  $.when(
    $.getJSON('fca-dashboard/data/fca_firms.json', data => { firmsData = data; }),
    $.getJSON('fca-dashboard/data/fca_names.json', data => { namesData = data; }),
    $.getJSON('fca-dashboard/data/fca_ars.json', data => { arsData = data; }),
    $.getJSON('fca-dashboard/data/fca_cf.json', data => { cfData = data; }),
    $.getJSON('fca-dashboard/data/fca_individuals_by_firm.json', data => { indivData = data; }),
    $.getJSON('fca-dashboard/data/fca_persons.json', data => { personsData = data; })
  ).then(initDashboard);

  // Tab switching
  $('.tab-btn').click(function(){
    $('.tab-btn').removeClass('active');
    $(this).addClass('active');
    $('.tab-content').removeClass('active');
    $('#' + $(this).data('tab')).addClass('active');
  });

  function initDashboard(){
    initFirmsTable();
    initIndividualsTable();
    initARsTable();
    initMatchesTable();
  }

  function initFirmsTable(){
    const tbl = $('#firms-table').DataTable({
      data: firmsData,
      paging: false,
      info: false,
      columns: [
        { data: 'frn',               title: 'FRN' },
        { data: 'organisation_name', title: 'Organisation Name' },
        { data: 'status',            title: 'Status' },
        { data: 'business_type',     title: 'Business Type' },
        { data: 'companies_house_number', title: 'CH#' },
        { data: d => (namesData[d.frn] || []).length, title: '#Names' },
        { data: d => (arsData[d.frn] || []).length, title: '#ARs' },
        { data: d => (cfData[d.frn] || []).filter(c => c.section === 'Current').length, title: '#CF Curr' },
        { data: d => (cfData[d.frn] || []).filter(c => c.section === 'Previous').length, title: '#CF Prev' },
        { data: d => (indivData[d.frn] || []).length, title: '#Inds' }
      ],
      order: [[1,'asc']]
    });

    // Click on Organisation Name cell (column 2) to toggle child row
    $('#firms-table tbody').on('click', 'td:nth-child(2)', function(){
      const tr  = $(this).closest('tr'),
            row = tbl.row(tr);

      if(row.child.isShown()){
        row.child.hide();
        tr.removeClass('shown');
      } else {
        row.child(renderFirmDetails(row.data())).show();
        tr.addClass('shown');
      }
    });
  }

  function renderFirmDetails(d){
    function renderList(title, arr){
      if(!arr || !arr.length) return '';
      return `<strong>${title}:</strong><ul>${arr.map(n=>`<li>${n||''}</li>`).join('')}</ul>`;
    }
    function renderTable(title, cols, data){
      if(!data || !data.length) return '';
      const hdr = cols.map(c=>`<th>${c}</th>`).join('');
      const body = data.map(r=>
        `<tr>${cols.map(c=>`<td>${r[c] != null ? r[c] : ''}</td>`).join('')}</tr>`
      ).join('');
      return `<strong>${title}:</strong>
        <table class="child-table"><thead><tr>${hdr}</tr></thead><tbody>${body}</tbody></table>`;
    }

    return `<div class="child-rows">
      ${renderList('Trading Names', namesData[d.frn])}
      ${renderTable('Appointed Reps', ['Name','Status'], arsData[d.frn])}
      ${renderTable('Controlled Functions', ['section','controlled_function','Individual Name','Effective Date'], cfData[d.frn])}
      ${renderTable('Firm Individuals', ['IRN','Name','Status'], indivData[d.frn])}
    </div>`;
  }

  function initIndividualsTable(){
    const allPersons = Object.values(personsData || {});
    const tbl = $('#individuals-table').DataTable({
      data: allPersons,
      paging: false,
      info: false,
      columns: [
        { data: 'irn',           title: 'IRN' },
        { data: 'name',          title: 'Name' },
        { data: 'status',        title: 'Status' },
        { data: 'date_of_birth', title: 'DoB' },
        {
          data: d => Object.entries(indivData || {})
                         .filter(([frn, arr]) => arr.some(e => e.IRN === d.irn))
                         .map(([frn]) => frn)
                         .join(', '),
          title: 'Firms'
        },
        {
          data: d => {
            let count = 0, name = d.name;
            Object.values(cfData || {}).forEach(arr =>
              arr.forEach(c => { if(c['Individual Name'] === name) count++; })
            );
            return count;
          },
          title: '#CF Records'
        }
      ],
      order: [[1,'asc']]
    });

    // Click Name to expand CF history
    $('#individuals-table tbody').on('click', 'td:nth-child(2)', function(){
      const tr  = $(this).closest('tr'),
            row = tbl.row(tr);
      if(row.child.isShown()){
        row.child.hide(); tr.removeClass('shown');
      } else {
        const d = row.data();
        // find all CF entries for this person
        const cfEntries = [];
        Object.values(cfData||{}).forEach(arr => arr.forEach(c => {
          if(c['Individual Name'] === d.name) cfEntries.push(c);
        }));
        const details = cfEntries.length
          ? `<table class="child-table"><thead><tr>
               <th>Section</th><th>Function</th><th>Effective Date</th>
             </tr></thead><tbody>${
               cfEntries.map(c=>`<tr><td>${c.section}</td><td>${c.controlled_function}</td><td>${c['Effective Date']}</td></tr>`).join('')
             }</tbody></table>`
          : `<div style="padding:0.5rem 1rem;"><em>No controlled-function records.</em></div>`;
        row.child(details).show(); tr.addClass('shown');
      }
    });
  }

  function initARsTable(){
    const allARs = [];
    Object.entries(arsData || {}).forEach(([frn, arr]) => {
      arr.forEach(r => {
        allARs.push({
          frn,
          name: r.Name,
          principal: r['Principal Firm Name'],
          effective: r['Effective Date'],
          url: r.URL
        });
      });
    });

    const tbl = $('#ars-table').DataTable({
      data: allARs,
      paging: false,
      info: false,
      columns: [
        { data: 'name',      title: 'Appointed Rep' },
        { data: 'principal', title: 'Principal Firm' }
      ],
      order: [[0,'asc']]
    });

    // Click name cell to expand Effective Date + link
    $('#ars-table tbody').on('click', 'td:nth-child(1)', function(){
      const tr  = $(this).closest('tr'),
            row = tbl.row(tr);
      if(row.child.isShown()){
        row.child.hide(); tr.removeClass('shown');
      } else {
        const d = row.data();
        const details = `
          <div style="padding:0.5rem 1rem;">
            <strong>Effective Date:</strong> ${d.effective}<br>
            ${d.url ? `<strong>Link:</strong> <a href="${d.url}" target="_blank">View on FCA</a>` : ''}
          </div>`;
        row.child(details).show(); tr.addClass('shown');
      }
    });
  }

  function initMatchesTable(){
    $('#matches-table').DataTable({
      data: [],
      paging: false,
      info: false,
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
