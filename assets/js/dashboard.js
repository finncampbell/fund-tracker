$(document).ready(function() {
  // Build the URL to the CSV
  const csvUrl = 'assets/data/master_companies.csv';

  // Inject a debug banner so we can see load results
  $('body').prepend(`
    <div id="ft-debug" 
         style="background:#ffecec;color:#900;padding:0.5em;text-align:center;">
      Loading…
    </div>
  `);

  Papa.parse(csvUrl, {
    download: true,
    header: true,
    complete: function(results) {
      // Show how many rows parsed
      $('#ft-debug').text(
        `Debug: loaded ${results.data.length} rows from CSV`
      );

      if (!results.data.length) {
        console.error('Fund Tracker: CSV parsed to zero rows', results);
        return;
      }

      // Initialize DataTable
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

      // Wire up filters
      $('.ft-btn').on('click', function() {
        $('.ft-btn').removeClass('active');
        $(this).addClass('active');
        const filter = $(this).data('filter') || '';
        table.column(4).search(filter).draw();
      });
    },
    error: function(err) {
      $('#ft-debug').text('Error loading CSV—see console.');
      console.error('Fund Tracker: PapaParse error', err);
    }
  });
});
