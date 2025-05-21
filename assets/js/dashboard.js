// assets/js/dashboard.js
$(document).ready(function() {
  const url = 'assets/data/relevant_companies.csv';
  // Match “Fund”, “GP”, “LLP” (first) or standalone “LP” (ignoring . - or space)
  const fundEntitiesRE = /\bFund\b|\bG[\.\-\s]?P\b|\bL[\.\-\s]?L[\.\-\s]?P\b|\bL[\.\-\s]?P\b/i;

  Papa.parse(url, { download: true, header: true, complete(results) {
    const data = results.data.filter(r => r['Company Number']);

    // Main companies table
    const companyTable = $('#companies').DataTable({
      data,
      columns: [
        { data: 'Company Name' },
        { data: 'Company Number' },
        { data: 'Incorporation Date' },
        { data: 'Category' },
        { data: 'Date Downloaded' }
      ],
      order: [[2, 'desc']],
      pageLength: 25,
      responsive: true
    });

    // SIC-enhanced table
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
      order: [[2, 'desc']],
      pageLength: 25,
      responsive: true
    });

    // Global filter hook
    $.fn.dataTable.ext.search.push((settings, rowData) => {
      const active = $('.ft-btn.active').data('filter') || '';

      // All: only show entries with Category ≠ "Other"
      if (!active) {
        return rowData[3] !== 'Other';
      }

      // SIC tab hides main table
      if (active === 'SIC') return false;

      // Fund Entities by regex on name
      if (active === 'Fund Entities') {
        return fundEntitiesRE.test(rowData[0]);
      }

      // Exact Category match for other tabs
      return rowData[3] === active;
    });

    // Tab click handler
    $('.ft-filters').on('click', '.ft-btn', function() {
      $('.ft-btn').removeClass('active');
      $(this).addClass('active');
      const filter = $(this).data('filter') || '';

      $('#companies-container').toggle(filter !== 'SIC');
      $('#sic-companies-container').toggle(filter === 'SIC');

      // Redraw main table on any non-SIC tab (including All)
      if (filter !== 'SIC') {
        companyTable.draw();
      }
    });
  }});
});
