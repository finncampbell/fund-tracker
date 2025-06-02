$(document).ready(function() {
  // Correct path for GitHub Pages with docs/ as root (NO leading slash!)
  const url = `assets/data/relevant_companies.csv?v=${Date.now()}`;
  const fundEntitiesRE = /\bFund\b|\bG[.\-\s]?P\b|\bL[.\-\s]?L[.\-\s]?P\b|\bL[.\-\s]?P\b/i;
  // Your Netlify site (where serverless functions live)
  const NETLIFY_BASE = 'https://fund-tracker-functions.netlify.app';

  Papa.parse(url, {
    download: true,
    header: true,
    complete(results) {
      // Filter out any rows lacking a CompanyNumber
      const raw = results.data.filter(r => r['CompanyNumber']);

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
        // Build normalized array of objects from parsed CSV rows
        const data = raw.map(r => {
          const num = r['CompanyNumber'].trim();
          return {
            CompanyName:       r['CompanyName'] || '',
            CompanyNumber:     num,
            IncorporationDate: r['IncorporationDate'] || '',
            Category:          r['Category'] || '',
            DateDownloaded:    r['DateDownloaded'] || '',
            SICCodes:          r['SIC Codes'] || '',
            SICDescription:    r['SIC Description'] || '',
            TypicalUseCase:    r['Typical Use Case'] || '',
            Directors:         directorsMap[num] || []
          };
        });

        // MAIN TABLE: show everything where Category ≠ "Other" and ≠ pure "SIC"
        const mainData = data.filter(r => {
          return r.Category !== 'Other' && r.Category !== 'SIC';
        });

        const companyTable = $('#companies').DataTable({
          data: mainData,
          columns: [
            { data: 'CompanyName',       title: 'Company Name' },
            { data: 'CompanyNumber',     title: 'Company Number' },
            { data: 'IncorporationDate', title: 'Incorporation Date' },
            { data: 'Category',          title: 'Category' },
            { data: 'DateDownloaded',    title: 'Date Downloaded' },
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

        // SIC TABLE: show everything where Category includes "SIC"
        const sicData = data.filter(r => {
          const parts = r.Category.split(',').map(s => s.trim());
          return parts.includes('SIC');
        });

        const sicTable = $('#sic-companies').DataTable({
          data: sicData,
          columns: [
            { data: 'CompanyName',       title: 'Company Name' },
            { data: 'CompanyNumber',     title: 'Company Number' },
            { data: 'IncorporationDate', title: 'Incorporation Date' },
            { data: 'Category',          title: 'Category' },
            { data: 'DateDownloaded',    title: 'Date Downloaded' },
            { data: 'SICCodes',          title: 'SIC Codes' },
            { data: 'SICDescription',    title: 'SIC Description' },
            { data: 'TypicalUseCase',    title: 'Typical Use Case' },
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

        // Expand/Collapse directors for both tables
        function toggleDirectors() {
          const $btn    = $(this);
          const tableId = $btn.closest('table').attr('id');
          const dt      = tableId === 'sic-companies' ? sicTable : companyTable;
          const row     = dt.row($btn.closest('tr'));

          if (row.child.isShown()) {
            // Hide child row
            row.child.hide();
            $btn.removeClass('active').text('Expand for Directors');
          } else {
            // Build child HTML for directors
            const dirs = row.data().Directors || [];
            let html = '<table class="child-table"><tr>' +
              '<th>Director Name</th>' +
              '<th>Appointment</th>' +
              '<th>Date of Birth</th>' +
              '<th># of Appointments</th>' +
              '<th>Officer Role</th>' +
              '<th>Nationality</th>' +
              '<th>Occupation</th>' +
              '<th>Details Link</th>' +
              '</tr>';

            dirs.forEach(d => {
              html += '<tr>' +
                `<td>${d.title || ''}</td>` +
                `<td>${d.appointment || ''}</td>` +
                `<td>${d.dateOfBirth || ''}</td>` +
                `<td>${d.appointmentCount || ''}</td>` +
                `<td>${d.officerRole || ''}</td>` +
                `<td>${d.nationality || ''}</td>` +
                `<td>${d.occupation || ''}</td>` +
                `<td><a href="https://api.company-information.service.gov.uk${d.selfLink}" target="_blank">Details</a></td>` +
                '</tr>';
            });
            html += '</table>';

            row.child(html).show();
            $btn.addClass('active').text('Hide Directors');
          }
        }

        $('#companies tbody').on('click', '.expand-btn', toggleDirectors);
        $('#sic-companies tbody').on('click', '.expand-btn', toggleDirectors);

        // DataTables search hook (MAIN table only)
        $.fn.dataTable.ext.search.push((settings, rowData) => {
          if (settings.nTable.id !== 'companies') return true;
          const active = $('.ft-btn.active').data('filter') || '';
          if (!active) {
            return rowData[3] !== 'Other' && rowData[3] !== 'SIC';
          }
          if (active === 'SIC') {
            return false;
          }
          if (active === 'Fund Entities') {
            return fundEntitiesRE.test(rowData[0]);
          }
          const parts = rowData[3].split(',').map(s => s.trim());
          return parts.includes(active);
        });

        // Filter‐tab click handler
        $('.ft-filters').on('click', '.ft-btn', function() {
          $('.ft-btn').removeClass('active');
          $(this).addClass('active');
          const filter = $(this).data('filter') || '';
          $('#companies-container').toggle(filter !== 'SIC');
          $('#sic-companies-container').toggle(filter === 'SIC');
          if (filter !== 'SIC') {
            companyTable.draw();
          }
        });
      } // initTables
    },   // parse complete
    error: function(err) {
      console.error('Error parsing relevant_companies.csv:', err);
    }
  });    // Papa.parse

  // Flatpickr for backfill range
  flatpickr("#backfill-range", {
    mode: "range",
    dateFormat: "Y-m-d",
    minDate: "2000-01-01",
    maxDate: new Date(),
    onChange: function(selectedDates, dateStr, instance) {
      const btn = document.getElementById("run-backfill");
      if (selectedDates.length === 2) {
        btn.disabled = false;
        btn.dataset.start = instance.formatDate(selectedDates[0], "Y-m-d");
        btn.dataset.end   = instance.formatDate(selectedDates[1], "Y-m-d");
      } else {
        btn.disabled = true;
        delete btn.dataset.start;
        delete btn.dataset.end;
      }
    }
  });

  // Backfill button logic
  document.getElementById("run-backfill").addEventListener("click", () => {
    const btn   = document.getElementById("run-backfill");
    const start = btn.dataset.start;
    const end   = btn.dataset.end;
    if (!start || !end) return;

    fetch(`${NETLIFY_BASE}/.netlify/functions/backfill`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
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
  });

  // Fetch Directors button logic
  document.getElementById("run-fetch-directors").addEventListener("click", async () => {
    const btn = document.getElementById("run-fetch-directors");
    btn.disabled = true;
    btn.textContent = 'Fetching Directors…';

    try {
      const resp = await fetch(
        `${NETLIFY_BASE}/.netlify/functions/trigger-fetch-directors`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({})
        }
      );

      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }

      alert('Fetch Directors workflow dispatched successfully!');
      // Optionally: if you have code that reloads directors.json / refreshes the tables, call it here.

    } catch (err) {
      console.error('Error dispatching Fetch Directors:', err);
      alert('Failed to dispatch Fetch Directors. See console for details.');
    } finally {
      btn.disabled = false;
      btn.textContent = 'Fetch Directors Now';
    }
  });
});
