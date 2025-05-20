// assets/js/dashboard.js
$(document).ready(function() {
  const url = 'assets/data/relevant_companies.csv';
  // whole-word + punctuation-tolerant + case-insensitive regex for Fund entities
  const fundEntitiesRE = '\\bFund\\b|\\bG\\W*P\\b|\\bL\\W*P\\b|\\bL\\W*L\\W*P\\b';

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

      $('.ft-btn').on('click', function() {
        $('.ft-btn').removeClass('active');
        $(this).addClass('active');
        const cat = $(this).data('filter') || '';

        if (cat === 'Fund Entities') {
          // regex, no smart-search, case-insensitive
          table.column(4).search(fundEntitiesRE, true, false, true).draw();
        } else {
          // simple substring, no smart-search, case-insensitive
          table.column(4).search(cat, false, false, true).draw();
        }
      });
    },
    error: function(err) {
      console.error('Error loading CSV:', err);
    }
  });
});
