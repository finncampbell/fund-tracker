// assets/js/dashboard.js
$(document).ready(function() {
  const url = 'assets/data/relevant_companies.csv';

  // Regex to detect Fund Entities anywhere in the **Company Name**
  const fundEntitiesRE = /\bF\W*U\W*N\W*D\b|\bG\W*P\b|\bL\W*P\b|\bL\W*L\W*P\b/i;

  Papa.parse(url, {
    download: true,
    header: true,
    complete: function(results) {
      const table = $('#companies').DataTable({
        data: results.data,
        columns: [
          { data: 'Company Name' },
          { data: 'Company Number' },
          { data: 'Incorporation Date' },
          { data: 'Status' },
          { data: 'Category' },
          { data: 'Date Downloaded' },
          { data: 'Time Discovered' }
        ],
        order: [[2, 'desc']],
        pageLength: 25,
        responsive: true
      });

      // Custom filter: checks the active button, and applies either:
      //  • Fund Entities → test Company Name against fundEntitiesRE
      //  • Any other → exact match on Category
      $.fn.dataTable.ext.search.push(function(settings, data, rowData) {
        const active = $('.ft-btn.active').data('filter') || '';
        if (!active) return true;  // “All” button

        if (active === 'Fund Entities') {
          return fundEntitiesRE.test(rowData['Company Name'] || '');
        }

        return rowData['Category'] === active;
      });

      $('.ft-btn').on('click', function() {
        $('.ft-btn').removeClass('active');
        $(this).addClass('active');
        table.draw();
      });
    },
    error: function(err) {
      console.error('Error loading CSV:', err);
    }
  });
});
