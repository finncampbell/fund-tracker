// assets/js/dashboard.js
$(document).ready(function() {
  const url = 'assets/data/relevant_companies.csv';
  const fundEntitiesRE = /\bFund\b|\bG\W*P\b|\bL\W*P\b|\bL\W*L\W*P\b/i;

  Papa.parse(url, { download: true, header: true, complete(results) {
    // 1) Clean out any blank rows from the CSV parse
    const data = results.data.filter(r => r['Company Number']);

    // 2) Initialize the main companies table
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

    // 3) Initialize the SIC‐enhanced table (only rows with a matched SIC Description)
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

    // 4) Add global filter hook for non-SIC tabs
    $.fn.dataTable.ext.search.push((settings, rowData) => {
      const active = $('.ft-btn.active').data('filter') || '';

      // “All” shows everything
      if (!active) return true;

      // On “SIC” tab hide the main table
      if (active === 'SIC') return false;

      // “Fund Entities” uses regex on name
      if (active === 'Fund Entities') {
        return fundEntitiesRE.test(rowData[0]);
      }

      // Otherwise exact Category match
      return rowData[3] === active;
    });

    // 5) Wire up tab clicks
    $('.ft-filters').on('click', '.ft-btn', function() {
      $('.ft-btn').removeClass('active');
      $(this).addClass('active');
      const filter = $(this).data('filter') || '';

      // Toggle visibility of the two tables
      $('#companies-container').toggle(filter !== 'SIC');
      $('#sic-companies-container').toggle(filter === 'SIC');

      // Redraw if needed
      if (filter && filter !== 'SIC') {
        companyTable.draw();
      }
    });
  }});
});
