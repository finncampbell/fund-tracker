// assets/js/dashboard.js
$(document).ready(function() {
  const url = 'assets/data/relevant_companies.csv';
  const fundEntitiesRE = /\bFund\b|\bG\W*P\b|\bL\W*P\b|\bL\W*L\W*P\b/i;

  // 1) Mandatory tabs that are always present and non-removable
  const mandatoryFilters = [
    { key: '',              label: 'All',           type: 'all'    },
    { key: 'Fund Entities', label: 'Fund Entities', pattern: fundEntitiesRE },
    { key: 'SIC',           label: 'SIC Codes',     type: 'sic'    }
  ];

  // 2) Additional, user-configurable tabs
  const additionalFilters = [
    { key: 'Ventures',    label: 'Ventures'    },
    { key: 'Capital',     label: 'Capital'     },
    { key: 'Equity',      label: 'Equity'      },
    { key: 'Advisors',    label: 'Advisors'    },
    { key: 'Partners',    label: 'Partners'    },
    { key: 'Investments', label: 'Investments' }
    // ← To add "Capital Management", just add:
    // { key: 'Capital Management', label: 'Capital Management' }
  ];

  // Combine so mandatoryFilters always come first
  const filters = [...mandatoryFilters, ...additionalFilters];

  // Render the filter buttons
  const $filtersDiv = $('.ft-filters').empty();
  filters.forEach((f, idx) => {
    const btn = $('<button>')
      .addClass('ft-btn')
      .attr('data-filter', f.key)
      .text(f.label);
    if (idx === 0) btn.addClass('active'); // "All" is active by default
    $filtersDiv.append(btn);
  });

  // Load the CSV once
  Papa.parse(url, { download: true, header: true, complete: function(results) {
    const data = results.data;

    // 1) Companies table (for all non-SIC tabs)
    const companyTable = $('#companies').DataTable({
      data,
      columns: [
        { data: 'Company Name' },
        { data: 'Company Number' },
        { data: 'Incorporation Date' },
        { data: 'Category' },
        { data: 'Date Downloaded' }
      ],
      order: [[2,'desc']],
      pageLength: 25,
      responsive: true
    });

    // 2) SIC-enhanced table (only rows with a matched SIC Description)
    const sicTable = $('#sic-companies').DataTable({
      data: data.filter(r => r['SIC Description']), 
      columns: [
        { data: 'Company Name' },
        { data: 'Company Number' },
        { data: 'Incorporation Date' },
        { data: 'Category' },
        { data: 'Date Downloaded' },
        { data: 'SIC Codes' },
        { data: 'SIC Description' },
        { data: 'Typical Use Case' }
      ],
      order: [[2,'desc']],
      pageLength: 25,
      responsive: true
    });

    // Global search hook for filtering the companies table
    $.fn.dataTable.ext.search.push((settings, rowData) => {
      const activeKey = $('.ft-btn.active').data('filter') || '';
      // Show everything on "All", hide companies table on SIC tab
      if (!activeKey || activeKey === 'SIC') return activeKey !== 'SIC';

      const name = rowData[0], cat = rowData[3];
      if (activeKey === 'Fund Entities')   return fundEntitiesRE.test(name);
      const f = filters.find(x => x.key === activeKey);
      if (f && f.pattern)                  return f.pattern.test(name) || f.pattern.test(cat);
      return cat === activeKey; // exact Category match
    });

    // Tab click handler
    $('.ft-btn').on('click', function() {
      $('.ft-btn').removeClass('active');
      $(this).addClass('active');
      const activeKey = $(this).data('filter') || '';

      // Toggle tables
      $('#companies-container').toggle(activeKey !== 'SIC');
      $('#sic-companies-container').toggle(activeKey === 'SIC');

      // Redraw companies table if needed
      if (activeKey && activeKey !== 'SIC') {
        companyTable.draw();
      }
    });
  }});
});
