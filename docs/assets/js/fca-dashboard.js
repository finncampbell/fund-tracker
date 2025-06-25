$(document).ready(function(){
  let firmsData, namesData, arsData, cfData, indivData, personsData;

  $.when(
    $.getJSON('data/fca_firms.json', data => { firmsData = data; }),
    $.getJSON('data/fca_names.json', data => { namesData = data; }),
    $.getJSON('data/fca_ars.json', data => { arsData = data; }),
    $.getJSON('data/fca_cf.json', data => { cfData = data; }),
    $.getJSON('data/fca_individuals_by_firm.json', data => { indivData = data; }),
    $.getJSON('data/fca_persons.json', data => { personsData = data; })
  ).then(initDashboard);

  $('.tabs button').click(function(){
    $('.tabs button').removeClass('active');
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
      columns: [
        { className:'dt-control', orderable:false, data:null, defaultContent:'' },
        { data:'frn',               title:'FRN' },
        { data:'organisation_name', title:'Organisation Name' },
        { data:'status',            title:'Status' },
        { data:'business_type',     title:'Business Type' },
        { data:'companies_house_number', title:'CH#' },
        { data: d=> (namesData[d.frn]||[]).length, title:'#Names' },
        { data: d=> (arsData[d.frn]||[]).length,   title:'#ARs' },
        { data: d=> (cfData[d.frn]||[]).filter(c=>c.section==='Current').length, title:'#CF Curr' },
        { data: d=> (cfData[d.frn]||[]).filter(c=>c.section==='Previous').length, title:'#CF Prev' },
        { data: d=> (indivData[d.frn]||[]).length, title:'#Inds' }
      ],
      order:[[1,'asc']]
    });

    $('#firms-table tbody').on('click','td.dt-control',function(){
      const row = tbl.row($(this).closest('tr'));
      row.child.isShown() ? row.child.hide() : row.child(renderFirmDetails(row.data())).show();
    });
  }

  function renderFirmDetails(d){
    function renderList(title, arr){
      if(!arr||arr.length===0) return '';
      return `<strong>${title}:</strong><ul>${arr.map(n=>`<li>${n||''}</li>`).join('')}</ul>`;
    }
    function renderTable(title, cols, data){
      if(!data||data.length===0) return '';
      const hdr = cols.map(c=>`<th>${c}</th>`).join('');
      const body = data.map(r=>`<tr>${cols.map(c=>`<td>${r[c]||''}</td>`).join('')}</tr>`).join('');
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
    const allPersons = Object.values(personsData||{});
    $('#individuals-table').DataTable({
      data: allPersons,
      columns: [
        { data:'irn',           title:'IRN' },
        { data:'name',          title:'Name' },
        { data:'status',        title:'Status' },
        { data:'date_of_birth', title:'DoB' },
        { data: d=> Object.entries(indivData||{})
                        .filter(([frn,arr])=>arr.some(e=>e.IRN===d.irn))
                        .map(([frn])=>frn).join(', '),
          title:'Firms' },
        { data: d=> {
            let count=0, nm=d.name;
            Object.values(cfData||{}).forEach(arr=>arr.forEach(c=>{ if(c['Individual Name']===nm) count++; }));
            return count;
          },
          title:'#CF Records'
        }
      ],
      order:[[1,'asc']]
    });
  }

  function initARsTable(){
    $('#ars-table').DataTable({
      data: arsData,
      columns: [
        { data: d=>d.FRN,                  title:'AR FRN' },
        { data: d=>d.Name,                 title:'Appointed Rep' },
        { data: d=>d['[NotinUse] Insurance Distribution'], title:'Insur. Dist.' },
        { data: d=> d['Principal FRN'],    title:'# Principals' }
      ],
      order:[[0,'asc']],
      createdRow(row,d){
        $(row)
          .css('cursor','pointer')
          .click(()=> {
            const table = $('#ars-table').DataTable();
            const r = table.row(row);
            r.child.isShown()
              ? r.child.hide()
              : r.child(renderARDetails(d)).show();
          });
      }
    });
  }

  function renderARDetails(d){
    return `<div class="child-rows">
      <strong>Principal FRN:</strong> ${d['Principal FRN']}<br/>
      <strong>Principal Firm Name:</strong> ${d['Principal Firm Name']}<br/>
      <strong>Effective Date:</strong> ${d['Effective Date']}
    </div>`;
  }

  function initMatchesTable(){
    $('#matches-table').DataTable({
      data: [],
      columns: [
        { title:'CH Entity' },
        { title:'Reg Date' },
        { title:'Match Type' },
        { title:'Matched To' },
        { title:'Link' }
      ]
    });
  }
});
