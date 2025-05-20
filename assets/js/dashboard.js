// assets/js/dashboard.js
$(document).ready(function() {
  const url = 'assets/data/relevant_companies.csv';

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
          // regex search for any of Fund, GP, LP, LLP
          table
            .column(4)
            .search('Fund|GP|LP|LLP', true, false)
            .draw();
        } else {
          table
            .column(4)
            .search(cat)
            .draw();
        }
      });
    },
    error: function(err) {
      console.error('Error loading CSV:', err);
    }
  });
});
