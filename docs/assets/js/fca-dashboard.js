// docs/assets/js/fca-dashboard.js
$(document).ready(function(){
  let firmsData, rawNames, rawARs, cfData, indivData, personsData;

  $.when(
    $.getJSON('/fca-dashboard/data/fca_firms.json', data => { firmsData = data; }),
    $.getJSON('/fca-dashboard/data/fca_names.json', data => { rawNames   = data; }),
    $.getJSON('/fca-dashboard/data/fca_ars.json',  data => { rawARs     = data; }),
    $.getJSON('/fca-dashboard/data/fca_cf.json',   data => { cfData     = data; }),
    $.getJSON('/fca-dashboard/data/fca_individuals_by_firm.json', data => { indivData = data; }),
    $.getJSON('/fca-dashboard/data/fca_persons.json', data => { personsData = data; })
  ).then(initDashboard);

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

  function getNamesByFrn(frn){
    if (Array.isArray(rawNames)) {
      return rawNames.filter(x => String(x.frn) === String(frn)).map(x => x.name);
    }
    return rawNames[frn] || [];
  }

  function normalizeARAppointments(){
    let appointments = Array.isArray(rawARs)
      ? rawARs
      : Object.values(rawARs).flat();
    const map = {};
    appointments.forEach(r => {
      const arFrn = String(r.FRN);
      if (!map[arFrn]) map[arFrn] = [];
      map[arFrn].push(r);
    });
    return map;
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
          title:'#Names'
        },
        {
          data: d => (normalizeARAppointments()[d.frn] || []).length,
          title:'#ARs'
        },
        {
          data: d => (cfData[d.frn]||[]).filter(c=>c.section==='Current').length,
          title:'#CF Curr'
        },
        {
          data: d => (cfData[d.frn]||[]).filter(c=>c.section==='Previous').length,
          title:'#CF Prev'
        },
        {
          data: d => (indivData[d.frn]||[]).length,
          title:'#Inds'
        }
      ],
      order: [[1,'asc']]
    });

    $('#firms-table tbody').on('click','td:nth-child(2)', function(){
      const tr = $(this).closest('tr'),
            row = tbl.row(tr);
      if (row.child.isShown()){
        row.child.hide(); tr.removeClass('shown');
      } else {
        row.child(renderFirmDetails(row.data())).show(); tr.addClass('shown');
      }
    });
  }

  function renderFirmDetails(d){
    const names = getNamesByFrn(d.frn),
          ars   = normalizeARAppointments()[d.frn] || [],
          cfs   = cfData[d.frn]||[],
          inds  = indivData[d.frn]||[];

    function renderList(title, arr){
      if (!arr.length) return '';
      return `<strong>${title}:</strong>
        <ul>${arr.map(n=>`<li>${n||''}</li>`).join('')}</ul>`;
    }
    function renderTable(title, cols, data){
      if (!data.length) return '';
      const hdr  = cols.map(c=>`<th>${c}</th>`).join(''),
            body = data.map(r=>
      `<tr>${cols.map(c=>`<td>${r[c]!=null?r[c]:''}</td>`).join('')}</tr>`).join('');
      return `<strong>${title}:</strong>
        <table class="child-table"><thead><tr>${hdr}</tr></thead><tbody>${body}</tbody></table>`;
    }

    return `<div class="child-rows">
      ${renderList('Trading Names', names)}
      ${renderTable('Appointed Reps', ['Name','Principal FRN','Principal Firm Name','Effective Date','EEA Tied Agent','Tied Agent','[NotinUse] Insurance Distribution'], ars)}
      ${renderTable('Controlled Functions', ['section','controlled_function','Individual Name','Effective Date'], cfs)}
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
            let cnt=0, nm=d.name;
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

    $('#individuals-table tbody').on('click','td:nth-child(2)', function(){
      const tr = $(this).closest('tr'),
            row= tbl.row(tr);
      if (row.child.isShown()){
        row.child.hide(); tr.removeClass('shown');
      } else {
        const d = row.data(),
              cfEntries = [];
        Object.values(cfData||{}).forEach(arr=>
          arr.forEach(c=>{ if(c['Individual Name']===d.name) cfEntries.push(c); })
        );
        const details = cfEntries.length
          ? `<table class="child-table"><thead><tr>
               <th>Section</th><th>Function</th><th>Effective Date</th>
             </tr></thead><tbody>${
               cfEntries.map(c=>`<tr><td>${c.section}</td><td>${c.controlled_function}</td><td>${c['Effective Date']}</td></tr>`).join('')
             }</tbody></table>`
          : `<div style="padding:0.5rem 1rem;"><em>No CF records</em></div>`;
        row.child(details).show(); tr.addClass('shown');
      }
    });
  }

  function initARsTable(){
    const apptMap = normalizeARAppointments();
    const allARs = Object.entries(apptMap).map(([arFrn, recs]) => ({
      arFrn,
      name: recs[0].Name,
      principalsCount: recs.length,
      insurDist: recs.some(r => String(r['[NotinUse] Insurance Distribution'])==='true')
    }));

    console.log('allARs →', allARs);  // verify shape

    const tbl = $('#ars-table').DataTable({
      data: allARs,
      paging: false,
      info: false,
      columns: [
        { data:'arFrn',           title:'AR FRN' },
        { data:'name',            title:'Appointed Rep' },
        { data:'principalsCount', title:'# Principals' },
        {
          data: d => d.insurDist ? '✓' : '✗',
          title:'Insur. Dist.',
          className:'dt-center'
        }
      ],
      order: [[1,'asc']]
    });

    $('#ars-table tbody').on('click','td:nth-child(2)', function(){
      const tr = $(this).closest('tr'),
            row= tbl.row(tr);
      if (row.child.isShown()){
        row.child.hide(); tr.removeClass('shown');
      } else {
        const d    = row.data(),
              recs = apptMap[d.arFrn] || [],
              hdr  = '<th>Principal FRN</th><th>Principal Name</th><th>Eff. Date</th><th>EEA-Tied</th><th>Tied-Agent</th>',
              body = recs.map(r=>
                `<tr>
                  <td>${r['Principal FRN']||''}</td>
                  <td>${r['Principal Firm Name']||''}</td>
                  <td>${r['Effective Date']||''}</td>
                  <td>${String(r['EEA Tied Agent']||false)}</td>
                  <td>${String(r['Tied Agent']||false)}</td>
                </tr>`
              ).join('');
        row.child(`<table class="child-table"><thead><tr>${hdr}</tr></thead><tbody>${body}</tbody></table>`).show();
        tr.addClass('shown');
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
