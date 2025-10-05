document.addEventListener('DOMContentLoaded', function() {
  const searchInput = document.getElementById('searchInput');
  if (!searchInput) return;

  const sections = [];
  document.querySelectorAll('h2').forEach(header => {
    const table = header.nextElementSibling;
    if (table && table.tagName === 'TABLE') {
      sections.push({
        header: header,
        table: table,
        rows: Array.from(table.querySelectorAll('tbody tr'))
      });
    }
  });

  searchInput.addEventListener('input', function(event) {
    const filterText = event.target.value.toLowerCase();

    if (filterText === '') {
      sections.forEach(section => {
        section.header.style.display = '';
        section.table.style.display = '';
        section.rows.forEach(row => {
          row.style.display = '';
          Array.from(row.getElementsByTagName('td')).forEach(cell => {
            cell.style.display = '';
          });
        });
      });
      return;
    }

    sections.forEach(section => {
      let sectionHasVisibleRows = false;

      section.rows.forEach(row => {
        const cells = row.getElementsByTagName('td');
        let rowHasMatch = false;
        const cellMatches = [];

        for (let i = 0; i < cells.length; i++) {
          const cell = cells[i];
          const cellText = cell.textContent.trim().toLowerCase();
          if (cellText && cellText.includes(filterText)) {
            cellMatches.push(true);
            rowHasMatch = true;
          } else {
            cellMatches.push(false);
          }
        }


        if (rowHasMatch) {
          row.style.display = '';
          sectionHasVisibleRows = true;

          for (let i = 0; i < cells.length; i++) {
            cells[i].style.display = cellMatches[i] ? '' : 'none';
          }
        } else {
          row.style.display = 'none';
        }
      });

      if (sectionHasVisibleRows) {
        section.header.style.display = '';
        section.table.style.display = '';
      } else {
        section.header.style.display = 'none';
        section.table.style.display = 'none';
      }
    });
  });
});
