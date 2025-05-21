// assets/js/dashboard.js
$(document).ready(function() {
  const url = 'assets/data/relevant_companies.csv';
  const fundEntitiesRE = /\bFund\b|\bG\W*P\b|\bL\W*P\b|\bL\W*L\W*P\b/i;

  Papa.parse(url, { download: true, header: true, complete(results) {
    const data = results.data.filter(r => r['Company Number']);

    // 1) Register the global filter hook BEFORE any table draw
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

    // 2) Initialize DataTables
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

    // 3) Force initial redraw so “All” filter takes effect immediately
    companyTable.draw();

    // 4) Tab click handler
    $('.ft-filters').on('click', '.ft-btn', function() {
      $('.ft-btn').removeClass('active');
      $(this).addClass('active');
      const filter = $(this).data('filter') || '';

      $('#companies-container').toggle(filter !== 'SIC');
      $('#sic-companies-container').toggle(filter === 'SIC');

      // Redraw main table on any non-SIC tab (including returning to All)
      if (filter !== 'SIC') {
        companyTable.draw();
      }
    });
  }});
});
