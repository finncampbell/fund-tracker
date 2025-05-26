$(document).ready(function() {
  // Cache‐busted URL so the browser always pulls fresh data
  const url = `assets/data/relevant_companies.csv?v=${Date.now()}`;
  const fundEntitiesRE = /\bFund\b|\bG[\.\-\s]?P\b|\bL[\.\-\s]?L[\.\-\s]?P\b|\bL[\.\-\s]?P\b/i;

  Papa.parse(url, {
    download: true,
    header: true,
    complete(results) {
      const raw = results.data.filter(r => r['Company Number']);

      let directorsMap = {};
      fetch('assets/data/directors.json')
        .then(r => r.json())
        .then(json => {
          directorsMap = Object.fromEntries(
            Object.entries(json).map(([k, v]) => [k.trim(), v])
          );
          initTables();
        })
        .catch(err => {
          console.error('Failed to load directors.json', err);
          initTables();
        });

      function initTables() {
        const data = raw.map(r => {
          const num = r['Company Number'].trim();
          return { ...r, 'Company Number': num, Directors: directorsMap[num] || [] };
        });

        // Main companies table
        const companyTable = $('#companies').DataTable({
          data,
          columns: [
            { data: 'Company Name' },
            { data: 'Company Number' },
            { data: 'Incorporation Date' },
            { data: 'Category' },
            { data: 'Date Downloaded' },
            {
              data: 'Directors',
              title: 'Directors',
              orderable: false,
              render: (dirs, type) =>
                type === 'display'
                  ? `<button class="expand-btn">Expand for Directors</button>`
                  : dirs
            }
          ],
          order: [[2, 'desc']],
          pageLength: 25,
          responsive: true
        });

        // SIC‐only companies table
        const sicData = data.filter(r => r['SIC Description']);
        const sicTable = $('#sic-companies').DataTable({
          data: sicData,
          columns: [
            { data: 'Company Name' },
            { data: 'Company Number' },
            { data: 'Incorporation Date' },
            { data: 'Category' },
            { data: 'Date Downloaded' },
            { data: 'SIC Codes' },
            { data: 'SIC Description' },
            { data: 'Typical Use Case' },
            {
              data: 'Directors',
              title: 'Directors',
              orderable: false,
              render: (dirs, type) =>
                type === 'display'
                  ? `<button class="expand-btn">Expand for Directors</button>`
                  : dirs
            }
          ],
          order: [[2, 'desc']],
          pageLength: 25,
          responsive: true
        });

        // Expand/Collapse directors
        function toggleDirectors() {
          const $btn = $(this);
          const tableId = $btn.closest('table').attr('id');
          const dt = tableId === 'sic-companies' ? sicTable : companyTable;
          const row = dt.row($btn.closest('tr'));

          if (row.child.isShown()) {
            row.child.hide();
            $btn.text('Expand for Directors');
          } else {
            const dirs = row.data().Directors;
            let html = '<table class="child-table"><tr>'
              +'<th>Director Name</th>'
              +'<th>Appointment</th>'
              +'<th>Date of Birth</th>'
              +'<th># of Appointments</th>'
              +'<th>Officer Role</th>'
              +'<th>Nationality</th>'
              +'<th>Occupation</th>'
              +'<th>Details Link</th>'
              +'</tr>';
            dirs.forEach(d => {
              html += `<tr>`
                +`<td>${d.title||''}</td>`
                +`<td>${d.appointment||''}</td>`
                +`<td>${d.dateOfBirth||''}</td>`
                +`<td>${d.appointmentCount||''}</td>`
                +`<td>${d.officerRole||''}</td>`
                +`<td>${d.nationality||''}</td>`
                +`<td>${d.occupation||''}</td>`
                +`<td><a href="https://api.company-information.service.gov.uk${d.selfLink}" target="_blank">Details</a></td>`
                +`</tr>`;
            });
            html += '</table>';
            row.child(html).show();
            $btn.text('Hide Directors');
          }
        }
        $('#companies tbody').on('click', '.expand-btn', toggleDirectors);
        $('#sic-companies tbody').on('click', '.expand-btn', toggleDirectors);

        // Filter hook
        $.fn.dataTable.ext.search.push((settings, rowData) => {
          if (settings.nTable.id !== 'companies') return true;
          const active = $('.ft-btn.active').data('filter') || '';
          if (!active) return true;                  // show all when no filter
          if (active === 'SIC') return false;
          if (active === 'Fund Entities') return fundEntitiesRE.test(rowData[0]);
          return rowData[3] === active;
        });

        // Tab click handler
        $('.ft-filters').on('click', '.ft-btn', function() {
          $('.ft-btn').removeClass('active');
          $(this).addClass('active');
          const filter = $(this).data('filter') || '';
          $('#companies-container').toggle(filter !== 'SIC');
          $('#sic-companies-container').toggle(filter === 'SIC');
          if (filter !== 'SIC') companyTable.draw();
        });
      }
    }
  });

  // Flatpickr range picker & backfill button
  flatpickr("#backfill-range", {
    mode: "range",
    dateFormat: "Y-m-d",
    minDate: "2000-01-01",
    maxDate: new Date(),
    onChange: function(selectedDates) {
      const btn = document.getElementById("run-backfill");
      if (selectedDates.length === 2) {
        btn.disabled = false;
        btn.dataset.start = flatpickr.formatDate(selectedDates[0], "Y-m-d");
        btn.dataset.end   = flatpickr.formatDate(selectedDates[1], "Y-m-d");
      } else {
        btn.disabled = true;
        delete btn.dataset.start;
        delete btn.dataset.end;
      }
    }
  });

  // Polls the backfill_status.json and updates the progress bar
  function pollBackfillStatus() {
    fetch(`assets/data/backfill_status.json?v=${Date.now()}`)
      .then(r => {
        if (!r.ok) throw new Error("Status not ready");
        return r.json();
      })
      .then(status => {
        const prog = document.getElementById("backfill-progress");
        const txt  = document.getElementById("backfill-progress-text");
        prog.max   = status.total;
        prog.value = status.processed;
        txt.textContent = `${status.processed} / ${status.total}`;

        if (status.processed < status.total) {
          setTimeout(pollBackfillStatus, 3000);
        } else {
          alert("Backfill complete!");
          document.getElementById("run-backfill").disabled = false;
        }
      })
      .catch(() => {
        // retry until the status file appears
        setTimeout(pollBackfillStatus, 2000);
      });
  }

  // Hook into backfill button to show progress and start polling
  document.getElementById("run-backfill").addEventListener("click", () => {
    // existing dispatch code
    const btn   = document.getElementById("run-backfill");
    const start = btn.dataset.start;
    const end   = btn.dataset.end;
    if (!start || !end) return;

    fetch('https://fund-tracker-functions.netlify.app/.netlify/functions/backfill', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ start_date: start, end_date: end })
    })
      .then(r => {
        if (!r.ok) throw new Error("Dispatch failed");
        alert(`Backfill started for ${start} → ${end}`);
        btn.disabled = true;
        document.getElementById("backfill-range").value = "";
      })
      .catch(err => {
        console.error(err);
        alert("Error starting backfill; see console.");
      });

    // show the progress bar and begin polling
    document.getElementById("backfill-status").style.display = "block";
    pollBackfillStatus();
  });
});
