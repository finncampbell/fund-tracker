$(document).ready(function(){
  let firmsData, namesData, arsData, cfData, indivData, personsData;

  // Fetch from the correct path under /fca-dashboard/data/
  $.when(
    $.getJSON('/fca-dashboard/data/fca_firms.json',        data=>{ firmsData = data; }),
    $.getJSON('/fca-dashboard/data/fca_names.json',        data=>{ namesData = data; }),
    $.getJSON('/fca-dashboard/data/fca_ars.json',          data=>{ arsData   = data; }),
    $.getJSON('/fca-dashboard/data/fca_cf.json',           data=>{ cfData    = data; }),
    $.getJSON('/fca-dashboard/data/fca_individuals_by_firm.json', data=>{ indivData = data; }),
    $.getJSON('/fca-dashboard/data/fca_persons.json',      data=>{ personsData= data; })
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
        { data:'frn', title:'FRN' },
        { data:'organisation_name', title:'Organisation Name' },
        { data:'status', title:'Status' },
        { data:'business_type', title:'Business Type' },
        { data:'companies_house_number', title:'CH#' },
        { data:d=>(namesData[d.frn]||[]).length, title:'#Names' },
        { data:d=>(arsData[d.frn]||[]).length, title:'#ARs' },
        { data:d=>(cfData[d.frn]||[]).filter(c=>c.section==='Current').length, title:'#CF Curr' },
        { data:d=>(cfData[d.frn]||[]).filter(c=>c.section==='Previous').length, title:'#CF Prev' },
        { data:d=>(indivData[d.frn]||[]).length, title:'#Inds' }
      ],
      order: [[0,'asc']],
      paging: false
    });

    $('#firms-table tbody').on('click','tr', function(){
      const row = tbl.row(this);
      if(row.child.isShown()) row.child.hide();
      else row.child(renderFirmDetails(row.data())).show();
    });
  }

  function renderFirmDetails(d){
    // same as before, showing Trading Names, ARs, CFs, Individuals
    /* … */
    return `<div class="child-rows">…</div>`;
  }

  function initIndividualsTable(){
    const allPersons = Object.values(personsData || {});
    $('#individuals-table').DataTable({
      data: allPersons,
      columns: [ /* … */ ],
      order:[[1,'asc']],
      paging:false
    });
  }

  function initARsTable(){
    // build an array of AR summaries from arsData
    const summary = Object.entries(arsData).map(([arFrn, list])=>{
      const repName = list[0] && list[0].Name;
      const insDist = list[0] && list[0]['[NotinUse] Insurance Distribution'];
      return {
        frn: arFrn,
        name: repName,
        count: list.length,
        insurance: insDist === 'true' ? 'Yes' : 'No',
        details: list
      };
    });

    const tbl = $('#ars-table').DataTable({
      data: summary,
      columns:[
        { data:'frn', title:'AR FRN' },
        { data:'name', title:'Appointed Rep' },
        { data:'count', title:'# Principals' },
        { data:'insurance', title:'Insur. Dist.' }
      ],
      paging:false
    });

    // click to expand child‐rows
    $('#ars-table tbody').on('click','tr',function(){
      const row = tbl.row(this);
      if(row.child.isShown()) row.child.hide();
      else {
        const d = row.data();
        const html = d.details.map(item=>
          `<div><strong>Principal FRN:</strong> ${item['Principal FRN']}<br>
           <strong>Principal Name:</strong> ${item['Principal Firm Name']}<br>
           <strong>Effective Date:</strong> ${item['Effective Date']}</div>`
        ).join('');
        row.child(html).show();
      }
    });
  }

  function initMatchesTable(){
    $('#matches-table').DataTable({
      data: [], columns:[ /* … */ ], paging:false
    });
  }
});
