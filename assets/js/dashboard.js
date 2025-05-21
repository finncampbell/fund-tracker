$(document).ready(function() {
  const url = 'assets/data/relevant_companies.csv';

  // 1) Define our tabs and their .test(row) functions
  const filters = [
    { key:'',              label:'All',           test: r => true },
    { key:'Fund Entities', label:'Fund Entities', test: r => /\bFund\b|\bG\W*P\b|\bL\W*P\b|\bL\W*L\W*P\b/i.test(r['Company Name']) },
    { key:'SIC',           label:'SIC Codes',     test: r => !!r['SIC Description'] },
    { key:'Ventures',      label:'Ventures',      test: r => /\bVenture(s)?\b/i.test(r['Company Name']) || /\bVenture(s)?\b/i.test(r['Category']) },
    { key:'Capital',       label:'Capital',       test: r => /\bCapital\b/i.test(r['Category']) },
    { key:'Equity',        label:'Equity',        test: r => /\bEquity\b/i.test(r['Category']) },
    { key:'Advisors',      label:'Advisors',      test: r => /\bAdvisors\b/i.test(r['Category']) },
    { key:'Partners',      label:'Partners',      test: r => /\bPartners\b/i.test(r['Category']) },
    { key:'Investments',   label:'Investments',   test: r => /\bInvestment(s)?\b/i.test(r['Company Name']) || /\bInvestment(s)?\b/i.test(r['Category']) }
  ];

  // 2) Scoring function
  function scoreRow(r) {
    let s = 0;
    const name = r['Company Name']||'', cat = r['Category']||'', desc = r['SIC Description']||'';
    if (/Venture(s)?/i.test(name))      s += 10;
    if (/Investment(s)?/i.test(name))   s +=  8;
    if (/\bFund\b/i.test(name))         s +=  6;
    if (/Partners?/i.test(name))        s +=  5;
    if (/Capital/i.test(cat))           s +=  4;
    if (/Equity/i.test(cat))            s +=  3;
    if (desc)                           s +=  2;
    return s;
  }

  Papa.parse(url, { download:true, header:true, complete(results) {
    let data = results.data.filter(r => r['Company Number']); 
    data = data.map(r => ({ ...r, _score: scoreRow(r) }));

    // Initialize DataTables empty
    const companyTable = $('#companies').DataTable({ data:[], columns:[
      { data:'Company Name' },{ data:'Company Number' },{ data:'Incorporation Date' },
      { data:'Category' },{ data:'Date Downloaded' },{ data:'_score', title:'Confidence' }
    ], order:[[5,'desc']], pageLength:25, responsive:true });

    const sicTable = $('#sic-companies').DataTable({ data:[], columns:[
      { data:'Company Name' },{ data:'Company Number' },{ data:'Incorporation Date' },
      { data:'Category' },{ data:'Date Downloaded' },{ data:'SIC Codes' },
      { data:'SIC Description' },{ data:'Typical Use Case' },{ data:'_score', title:'Confidence' }
    ], order:[[8,'desc']], pageLength:25, responsive:true });

    // Render for a given filter idx
    function renderFilter(idx) {
      const { key, test } = filters[idx];
      const all = data.map(r => ({ ...r, _matched: test(r) }));
      all.sort((a,b)=> (b._matched - a._matched) || (b._score - a._score));
      if (key==='SIC') {
        $('#companies-container').hide(); $('#sic-companies-container').show();
        sicTable.clear().rows.add(all.filter(r=>r._matched)).draw();
      } else {
        $('#sic-companies-container').hide(); $('#companies-container').show();
        companyTable.clear().rows.add(all).draw();
      }
    }

    // Build buttons
    const $fb = $('.ft-filters').empty();
    filters.forEach((f,i)=>{
      const btn = $(`<button class="ft-btn" data-idx="${i}">${f.label}</button>`);
      if(i===0) btn.addClass('active');
      $fb.append(btn);
    });

    // Click handler
    $fb.on('click','.ft-btn',function(){
      $fb.find('.ft-btn').removeClass('active');
      $(this).addClass('active');
      renderFilter(+$(this).attr('data-idx'));
    });

    // Initial render
    renderFilter(0);
  }});
});
