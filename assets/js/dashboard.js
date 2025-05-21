// assets/js/dashboard.js
$(document).ready(function() {
  const url = 'assets/data/relevant_companies.csv';
  const fundEntitiesRE = /\bFund\b|\bG\W*P\b|\bL\W*P\b|\bL\W*L\W*P\b/i;

  Papa.parse(url, { download: true, header: true, complete(results) {
    const data = results.data.filter(r => r['Company Number']);

    // Initialize main companies table
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

    // Initialize SIC-enhanced table
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

    // Global filter hook for the companies table
    $.fn.dataTable.ext.search.push((settings, rowData) => {
      const active = $('.ft-btn.active').data('filter') || '';
      if (!active) return true;            // All
      if (active === 'SIC') return false;  // hide on SIC tab
      if (active === 'Fund Entities') {
        return fundEntitiesRE.test(rowData[0]);
      }
      return rowData[3] === active;        // exact Category match
    });

    // Tab click handler
    $('.ft-filters').on('click', '.ft-btn', function() {
      $('.ft-btn').removeClass('active');
      $(this).addClass('active');
      const filter = $(this).data('filter') || '';

      // Toggle table visibility
      $('#companies-container').toggle(filter !== 'SIC');
      $('#sic-companies-container').toggle(filter === 'SIC');

      // ALWAYS redraw the companies table when it's visible (including All)
      if (filter !== 'SIC') {
        companyTable.draw();
      }
    });
  }});
});
