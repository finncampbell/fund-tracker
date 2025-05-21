$(document).ready(function() {
  const url = 'assets/data/relevant_companies.csv';

  // 1) Define our tabs and their .test(row) functions
  const filters = [
    { key:'',              label:'All',           test: r => true },
    { key:'Fund Entities', label:'Fund Entities', test: r => (
        /\bFund\b/i.test(r['Company Name'])
        || /\bG[\.\-\s]?P\b/i.test(r['Company Name'])
        || /\bL[\.\-\s]?P\b/i.test(r['Company Name'])
        || /\bL[\.\-\s]?L[\.\-\s]?P\b/i.test(r['Company Name'])
      )
    },
    { key:'SIC',           label:'SIC Codes',     test: r => !!r['SIC Description'] },
    { key:'Ventures',      label:'Ventures',      test: r => /\bVenture(s)?\b/i.test(r['Company Name']) || /\bVenture(s)?\b/i.test(r['Category']) },
    { key:'Capital',       label:'Capital',       test: r => /\bCapital\b/i.test(r['Category']) },
    { key:'Equity',        label:'Equity',        test: r => /\bEquity\b/i.test(r['Category']) },
    { key:'Advisors',      label:'Advisors',      test: r => /\bAdvisors\b/i.test(r['Category']) },
    { key:'Partners',      label:'Partners',      test: r => /\bPartners\b/i.test(r['Category']) },
    { key:'Investments',   label:'Investments',   test: r => /\bInvestment(s)?\b/i.test(r['Company Name']) || /\bInvestment(s)?\b/i.test(r['Category']) }
  ];

  // 2) Scoring function: bucketed 0/70/100
  const KEYWORDS = [
    'Venture','Ventures',
    'Investment','Investments',
    'Capital','Equity','Advisors','Partners',
    'Fund','GP','LP','LLP'
  ];

  function scoreRow(r) {
    const text = `${r['Company Name'] || ''} ${r['Category'] || ''}`;
    const hasSIC = Boolean(r['SIC Description']);
    let keywordMatches = 0;
    for (let kw of KEYWORDS) {
      const pattern = kw.length === 2
        ? new RegExp(`\\b${kw[0]}[\\.\\-\\s]?${kw[1]}\\b`, 'i')
        : new RegExp(`\\b${kw}\\b`, 'i');
      if (pattern.test(text)) keywordMatches++;
    }
    if (keywordMatches > 1 || (keywordMatches === 1 && hasSIC)) {
      return 100;
    } else if (keywordMatches === 1) {
      return 70;
    } else {
      return 0;
    }
  }

  // 3) Load and parse CSV, attach scores
  Papa.parse(url, { download:true, header:true, complete(results) {
    let data = results.data.filter(r => r['Company Number']);
    data = data.map(r => ({ ...r, _score: scoreRow(r) }));

    // 4) Initialize DataTables: sort by Incorporation Date (col 2)
    const companyTable = $('#companies').DataTable({
      data: [],
      columns: [
        { data:'Company Name' },
        { data:'Company Number' },
        { data:'Incorporation Date' },
        { data:'Category' },
        { data:'Date Downloaded' },
        { data:'_score', title:'Confidence' }
      ],
      order: [[2,'desc']],
      pageLength:25,
      responsive:true
    });

    const sicTable = $('#sic-companies').DataTable({
      data: [],
      columns: [
        { data:'Company Name' },
        { data:'Company Number' },
        { data:'Incorporation Date' },
        { data:'Category' },
        { data:'Date Downloaded' },
        { data:'SIC Codes' },
        { data:'SIC Description' },
        { data:'Typical Use Case' },
        { data:'_score', title:'Confidence' }
      ],
      order: [[2,'desc']],
      pageLength:25,
      responsive:true
    });

    // 5) Render function: sort by Incorporation Date only
    function renderFilter(idx) {
      const { key, test } = filters[idx];
      const all = data.map(r => ({ ...r, _matched: test(r) }));
      all.sort((a, b) =>
        new Date(b['Incorporation Date']) - new Date(a['Incorporation Date'])
      );

      if (key === 'SIC') {
        $('#companies-container').hide();
        $('#sic-companies-container').show();
        sicTable.clear().rows.add(all.filter(r => r._matched)).draw();
      } else {
        $('#sic-companies-container').hide();
        $('#companies-container').show();
        companyTable.clear().rows.add(all).draw();
      }
    }

    // 6) Build filter buttons and click handler
    const $fb = $('.ft-filters').empty();
    filters.forEach((f,i) => {
      const btn = $(`<button class="ft-btn" data-idx="${i}">${f.label}</button>`);
      if (i === 0) btn.addClass('active');
      $fb.append(btn);
    });

    $fb.on('click', '.ft-btn', function() {
      $fb.find('.ft-btn').removeClass('active');
      $(this).addClass('active');
      renderFilter(+$(this).attr('data-idx'));
    });

    // Initial render
    renderFilter(0);
  }});
});
