$(document).ready(function(){
  let firmsData, rawNames, rawARs, cfData, indivData, personsData;

  // load everything in parallel
  $.when(
    $.getJSON('fca-dashboard/data/fca_firms.json', data => { firmsData = data; }),
    $.getJSON('fca-dashboard/data/fca_names.json', data => { rawNames = data; }),
    $.getJSON('fca-dashboard/data/fca_ars.json', data => { rawARs = data; }),
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

  // Helper: fetch trading names by FRN
  function getNamesByFrn(frn){
    if(Array.isArray(rawNames)){
      // rawNames is [{frn,name},…]
      return rawNames.filter(x=>x.frn===frn).map(x=>x.name);
    } else {
      // rawNames is { frn: [names], … }
      return rawNames[frn]||[];
    }
  }

  // Helper: fetch AR entries by FRN
  function getARsByFrn(frn){
    if(Array.isArray(rawARs)){
      // rawARs is flat array
      return rawARs.filter(r => String(r['Principal FRN']) === String(frn));
    } else {
      // rawARs is { frn: [entries], … }
      return rawARs[frn] || [];
    }
  }

  function initFirmsTable(){
    const tbl = $('#firms-table').DataTable({
      data: firmsData,
      paging: false,
      info: false,
      columns: [
        { data:'frn',               title:'FRN' },
        { data:'organisation_name', title:'Organisation Name' },
        { data:'status',            title:'Status' },
        { data:'business_type',     title:'Business Type' },
        { data:'companies_house_number', title:'CH#' },
        {
          data: d => getNamesByFrn(d.frn).length,
          title: '#Names'
        },
        {
          data: d => getARsByFrn(d.frn).length,
          title: '#ARs'
        },
        {
          data: d => (cfData[d.frn]||[]).filter(c=>c.section==='Current').length,
          title: '#CF Curr'
        },
        {
          data: d => (cfData[d.frn]||[]).filter(c=>c.section==='Previous').length,
          title: '#CF Prev'
        },
        {
          data: d => (indivData[d.frn]||[]).length,
          title: '#Inds'
        }
      ],
      order: [[1,'asc']]
    });

    // click on Organisation Name (2nd cell) to expand
    $('#firms-table tbody').on('click','td:nth-child(2)', function(){
      const tr = $(this).closest('tr'),
            row = tbl.row(tr);
      if(row.child.isShown()){
        row.child.hide(); tr.removeClass('shown');
      } else {
        row.child(renderFirmDetails(row.data())).show(); tr.addClass('shown');
      }
    });
  }

  function renderFirmDetails(d){
    const names = getNamesByFrn(d.frn),
          ars   = getARsByFrn(d.frn),
          cfs   = cfData[d.frn]||[],
          inds  = indivData[d.frn]||[];

    function renderList(title, arr){
      if(!arr.length) return '';
      return `<strong>${title}:</strong>
        <ul>${arr.map(n=>`<li>${n||''}</li>`).join('')}</ul>`;
    }
    function renderTable(title, cols, data){
      if(!data.length) return '';
      const hdr = cols.map(c=>`<th>${c}</th>`).join(''),
            body= data.map(r=>
              `<tr>${cols.map(c=>`<td>${r[c]!=null?r[c]:''}</td>`).join('')}</tr>`
            ).join('');
      return `<strong>${title}:</strong>
        <table class="child-table"><thead><tr>${hdr}</tr></thead><tbody>${body}</tbody></table>`;
    }

    return `<div class="child-rows">
      ${renderList('Trading Names', names)}
      ${renderTable('Appointed Reps', ['Name','Principal Firm Name','Effective Date'], ars)}
      ${renderTable('Controlled Functions',
                    ['section','controlled_function','Individual Name','Effective Date'],
                    cfs)}
      ${renderTable('Firm Individuals', ['IRN','Name','Status'], inds)}
    </div>`;
  }

  function initIndividualsTable(){
    const allPersons = Object.values(personsData||{});

    const tbl = $('#individuals-table').DataTable({
      data: allPersons,
      paging: false,
      info: false,
      columns: [
        { data:'irn',           title:'IRN' },
        { data:'name',          title:'Name' },
        { data:'status',        title:'Status' },
        { data:'date_of_birth', title:'DoB' },
        {
          data: d => Object.entries(indivData||{})
                         .filter(([frn,arr])=>arr.some(e=>e.IRN===d.irn))
                         .map(([frn])=>frn).join(', '),
          title:'Firms'
        },
        {
          data: d => {
            let cnt=0;
            const nm = d.name;
            Object.values(cfData||{}).forEach(arr=>
              arr.forEach(c=>{ if(c['Individual Name']===nm) cnt++; })
            );
            return cnt;
          },
          title:'#CF Records'
        }
      ],
      order: [[1,'asc']]
    });

    // click on Name (2nd cell) to expand CF history
    $('#individuals-table tbody').on('click','td:nth-child(2)', function(){
      const tr = $(this).closest('tr'),
            row= tbl.row(tr);
      if(row.child.isShown()){
        row.child.hide(); tr.removeClass('shown');
      } else {
        const d = row.data();
        // gather CF entries for this person
        const cfEntries = [];
        Object.values(cfData||{}).forEach(arr=>
          arr.forEach(c=>{
            if(c['Individual Name']===d.name) cfEntries.push(c);
          })
        );
        const details = cfEntries.length
          ? `<table class="child-table"><thead><tr>
               <th>Section</th><th>Function</th><th>Effective Date</th>
             </tr></thead><tbody>${
               cfEntries.map(c=>`<tr><td>${c.section}</td>
                                  <td>${c.controlled_function}</td>
                                  <td>${c['Effective Date']}</td></tr>`).join('')
             }</tbody></table>`
          : `<div style="padding:0.5rem 1rem;"><em>No CF records</em></div>`;
        row.child(details).show(); tr.addClass('shown');
      }
    });
  }

  function initARsTable(){
    // Use rawARs array or object
    const allARs = Array.isArray(rawARs)
      ? rawARs.map(r=> ({
          name: r.Name,
          principal: r['Principal Firm Name'],
          effective: r['Effective Date'],
          url: r.URL
        }))
      : Object.values(rawARs).flat().map(r=>({
          name: r.Name,
          principal: r['Principal Firm Name'],
          effective: r['Effective Date'],
          url: r.URL
        }));

    const tbl = $('#ars-table').DataTable({
      data: allARs,
      paging: false,
      info: false,
      columns: [
        { data:'name',      title:'Appointed Rep' },
        { data:'principal', title:'Principal Firm' }
      ],
      order: [[0,'asc']]
    });

    // click on Appointed Rep name (1st cell) to expand
    $('#ars-table tbody').on('click','td:nth-child(1)', function(){
      const tr = $(this).closest('tr'),
            row= tbl.row(tr);
      if(row.child.isShown()){
        row.child.hide(); tr.removeClass('shown');
      } else {
        const d = row.data();
        const details = `<div style="padding:0.5rem 1rem;">
          <strong>Effective Date:</strong> ${d.effective}<br>
          ${d.url? `<strong>Link:</strong> <a href="${d.url}" target="_blank">View</a>` : ''}
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
        { title:'CH Entity' },
        { title:'Reg Date' },
        { title:'Match Type' },
        { title:'Matched To' },
        { title:'Link' }
      ]
    });
  }
});
