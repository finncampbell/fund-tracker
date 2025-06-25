$(document).ready(function(){
  let firmsData, namesData, arsData, cfData, indivData, personsData;

  $.when(
    $.getJSON('fca-dashboard/data/fca_firms.json',    d=>firmsData=d),
    $.getJSON('fca-dashboard/data/fca_names.json',    d=>namesData=d),
    $.getJSON('fca-dashboard/data/fca_ars.json',      d=>arsData=d),
    $.getJSON('fca-dashboard/data/fca_cf.json',       d=>cfData=d),
    $.getJSON('fca-dashboard/data/fca_individuals_by_firm.json', d=>indivData=d),
    $.getJSON('fca-dashboard/data/fca_persons.json',  d=>personsData=d)
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
    $('#firms-table').DataTable({
      data: firmsData,
      paging: false,
      ordering: true,
      info: false,
      columns: [
        { data:'frn',               title:'FRN' },
        { data:'organisation_name', title:'Organisation Name'},
        { data:'status',            title:'Status' },
        { data:'business_type',     title:'Business Type' },
        { data:'companies_house_number', title:'CH#' },
        { data: d=> (namesData[d.frn]||[]).length, title:'#Names' },
        { data: d=> (arsData[d.frn]||[]).length,   title:'#ARs' },
        { data: d=> (cfData[d.frn]||[]).filter(c=>c.section==='Current').length, title:'#CF Curr' },
        { data: d=> (cfData[d.frn]||[]).filter(c=>c.section==='Previous').length, title:'#CF Prev' },
        { data: d=> (indivData[d.frn]||[]).length, title:'#Inds' }
      ],
      order: [[1,'asc']]
    });
  }

  function initIndividualsTable(){
    const allPersons = Object.values(personsData||{});
    $('#individuals-table').DataTable({
      data: allPersons,
      paging: false,
      info: false,
      columns: [
        { data:'irn',           title:'IRN' },
        { data:'name',          title:'Name' },
        { data:'status',        title:'Status' },
        { data:'date_of_birth', title:'DoB' },
        { data: d=> Object.entries(indivData||{})
                         .filter(([_,arr])=>arr.some(e=>e.IRN===d.irn))
                         .map(([frn])=>frn).join(', '),
          title:'Firms'
        },
        { data: d=>{
            let cnt=0;
            Object.values(cfData||[]).forEach(arr=>
              arr.forEach(c=>{ if(c['Individual Name']===d.name) cnt++; })
            );
            return cnt;
          }, title:'#CF Records'
        }
      ],
      order: [[1,'asc']]
    });
  }

  function initARsTable(){
    // Flatten each FRN’s ARs into one array with frn field
    const allARs = [];
    Object.entries(arsData||{}).forEach(([frn, arr])=>{
      arr.forEach(r=> allARs.push({...r, frn}));
    });
    $('#ars-table').DataTable({
      data: allARs,
      paging: false,
      info: false,
      columns: [
        { data:'frn',   title:'FRN' },
        { data:'Name',  title:'AR Name' },
        { data:'Status',title:'Status' },
        { data:'URL',   title:'Link',
          render: u=> u ? `<a href="${u}" target="_blank">View</a>` : ''
        }
      ],
      order: [[0,'asc']]
    });
  }

  function initMatchesTable(){
    $('#matches-table').DataTable({
      data: [],  /* will replace when ready */
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
