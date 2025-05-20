// assets/js/dashboard.js
$(document).ready(function() {
  const url = 'assets/data/relevant_companies.csv';

  // regex literal: whole-word, punctuation-tolerant, case-insensitive
  // matches Fund (with optional punctuation), GP, LP or LLP
  const fundEntitiesRegex = /\b(?:F\W*U\W*N\W*D|G\W*P|L\W*P|L\W*L\W*P)\b/i;

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
          // use regex literal (with its own 'i' flag) to catch punctuation variants
          table
            .column(4)
            .search(fundEntitiesRegex, true, false)
            .draw();
        } else {
          // normal case-insensitive substring search for other buttons
          table
            .column(4)
            .search(cat, false, false, true)
            .draw();
        }
      });
    },
    error: function(err) {
      console.error('Error loading CSV:', err);
    }
  });
});
