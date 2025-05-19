$(document).ready(function() {
  const csvUrl = 'assets/data/master_companies.csv';

  Papa.parse(csvUrl, {
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
          { data: 'Source' },
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
        const filter = $(this).data('filter') || '';
        table.column(4).search(filter).draw();
      });
    },
    error: function(err) {
      console.error('Error loading CSV:', err);
    }
  });
});
