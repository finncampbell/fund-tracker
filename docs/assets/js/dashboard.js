$(document).ready(function() {
  // Correct path for GitHub Pages with docs/ as root (NO leading slash!)
  const url = `assets/data/relevant_companies.csv?v=${Date.now()}`;
  const fundEntitiesRE = /\bFund\b|\bG[.\-\s]?P\b|\bL[.\-\s]?L[.\-\s]?P\b|\bL[.\-\s]?P\b/i;

  Papa.parse(url, {
    download: true,
    header: true,
    complete(results) {
      // Filter out any “empty” rows
      const raw = results.data.filter(r => r['CompanyNumber']);

      let directorsMap = {};
      fetch('assets/data/directors.json')
        .then(r => r.json())
        .then(json => {
          // Trim keys in case there are stray spaces
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
        // Normalize each row into an object with matching keys
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

        // Main companies table (everything except Category === "SIC")
        const filteredMainData = data.filter(r => r.Category !== 'SIC');
        const companyTable = $('#companies').DataTable({
          data: filteredMainData,
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

        // SIC-only companies table (Category === "SIC")
        const sicData = data.filter(r => r.Category === 'SIC');
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
                +`<td>${d.title || ''}</td>`
                +`<td>${d.appointment || ''}</td>`
                +`<td>${d.dateOfBirth || ''}</td>`
                +`<td>${d.appointmentCount || ''}</td>`
                +`<td>${d.officerRole || ''}</td>`
                +`<td>${d.nationality || ''}</td>`
                +`<td>${d.occupation || ''}</td>`
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

        // Filter hook for “Non‐Other” in main table (categories other than “SIC”)
        $.fn.dataTable.ext.search.push((settings, rowData) => {
          if (settings.nTable.id !== 'companies') return true;
          const active = $('.ft-btn.active').data('filter') || '';
          if (!active) {
            // “All” button: show everything except rows with Category “Other”
            return rowData[3] !== 'Other';
          }
          if (active === 'SIC') {
            // Handled by toggling the SIC table
            return false;
          }
          if (active === 'Fund Entities') {
            // Matches by company name
            return fundEntitiesRE.test(rowData[0]);
          }
          // Otherwise, match exact category
          return rowData[3] === active;
        });

        // Tab click handler (filters + show/hide appropriate table)
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
      }
    }
  });

  // Flatpickr range picker & backfill button
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

  document.getElementById("run-backfill").addEventListener("click", () => {
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
  });
});
