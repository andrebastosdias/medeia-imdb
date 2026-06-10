const DATA_URLS = {
  movies: "../data/movies.csv",
  sessions: "../data/sessions.csv",
  imdb: "../data/imdb.csv",
};

const STORAGE_KEY = "medeia-imdb.table-state.v1";
const COLUMN_SCHEMA_VERSION = 6;

const columns = {
  sessions: [
    { key: "id", label: "ID", type: "number" },
    { key: "original_title", label: "Original title" },
    { key: "title", label: "Title" },
    { key: "director", label: "Director" },
    { key: "cast", label: "Cast", list: true },
    { key: "release_year", label: "Release year", type: "number" },
    { key: "runtime", label: "Runtime" },
    { key: "session", label: "Session", type: "date" },
    { key: "sessions_left", label: "Sessions left", type: "number" },
    { key: "watched", label: "Watched", type: "boolean" },
    { key: "in_imdb_list", label: "In IMDb list", type: "boolean" },
    { key: "links", label: "Links", type: "links", filterable: false, sortable: false },
  ],
  movies: [
    { key: "id", label: "ID", type: "number" },
    { key: "original_title", label: "Original title" },
    { key: "title", label: "Title" },
    { key: "director", label: "Director" },
    { key: "cast", label: "Cast", list: true },
    { key: "release_year", label: "Release year", type: "number" },
    { key: "runtime", label: "Runtime" },
    { key: "sessions", label: "Sessions", type: "number" },
    { key: "imdb_id", label: "IMDb ID" },
    { key: "watched", label: "Watched", type: "boolean" },
    { key: "imdb_lists", label: "IMDb lists", list: true },
    { key: "links", label: "Links", type: "links", filterable: false, sortable: false },
  ],
};

const sharedColumnKeys = [
  "id",
  "original_title",
  "title",
  "director",
  "cast",
  "release_year",
  "runtime",
  "watched",
  "links",
];

const defaultState = {
  activeTab: "sessions",
  globalSearch: "",
  columnSchemaVersion: COLUMN_SCHEMA_VERSION,
  visibleColumns: getDefaultVisibleColumns(),
  tables: {
    sessions: { sort: { key: "session", direction: "asc" }, filters: {} },
    movies: { sort: { key: "title", direction: "asc" }, filters: {} },
  },
};

const runtime = {
  data: { sessions: [], movies: [] },
  filtered: { sessions: [], movies: [] },
  state: loadState(),
  openFilter: null,
  columnsOpen: false,
  filterSearch: {},
  toastTimer: null,
  toastHideTimer: null,
};

const els = {
  status: document.querySelector("#status"),
  globalSearch: document.querySelector("#global-search"),
  columnsToggle: document.querySelector("#columns-toggle"),
  clearFilters: document.querySelector("#clear-filters"),
  tabs: document.querySelectorAll(".tab-button"),
  panels: {
    sessions: document.querySelector("#sessions-panel"),
    movies: document.querySelector("#movies-panel"),
  },
  tables: {
    sessions: document.querySelector("#sessions-table"),
    movies: document.querySelector("#movies-table"),
  },
  counts: {
    sessions: document.querySelector("#sessions-count"),
    movies: document.querySelector("#movies-count"),
  },
  popover: document.querySelector("#filter-popover"),
  columnsPopover: document.querySelector("#columns-popover"),
  toast: document.querySelector("#toast"),
};

document.addEventListener("DOMContentLoaded", init);

async function init() {
  bindGlobalEvents();

  try {
    const [moviesCsv, sessionsCsv, imdbCsv] = await Promise.all([
      fetchText(DATA_URLS.movies),
      fetchText(DATA_URLS.sessions),
      fetchText(DATA_URLS.imdb),
    ]);

    const movies = parseCsv(moviesCsv);
    const sessions = parseCsv(sessionsCsv);
    const imdb = parseCsv(imdbCsv);
    runtime.data = buildTables(movies, sessions, imdb);

    els.status.textContent = "Loaded movies, sessions, and IMDb data.";
    render();
  } catch (error) {
    els.status.classList.add("error");
    els.status.textContent = `Could not load CSV data: ${error.message}`;
  }
}

function bindGlobalEvents() {
  els.globalSearch.addEventListener("input", () => {
    runtime.state.globalSearch = els.globalSearch.value;
    saveState();
    render();
  });

  els.clearFilters.addEventListener("click", () => {
    runtime.state.globalSearch = "";
    for (const tableName of Object.keys(runtime.state.tables)) {
      runtime.state.tables[tableName].filters = {};
      runtime.state.tables[tableName].sort = { key: "id", direction: "asc" };
    }
    runtime.openFilter = null;
    saveState();
    render();
  });

  els.columnsToggle.addEventListener("click", (event) => {
    event.stopPropagation();
    runtime.columnsOpen = !runtime.columnsOpen;
    runtime.openFilter = null;
    render();
  });

  for (const button of els.tabs) {
    button.addEventListener("click", () => {
      runtime.state.activeTab = button.dataset.tab;
      saveState();
      render();
    });
  }

  document.addEventListener("click", (event) => {
    if (
      runtime.openFilter &&
      !event.target.closest(".filter-popover") &&
      !event.target.closest(".filter-button")
    ) {
      runtime.openFilter = null;
      renderFilterPopover();
    }
    if (
      runtime.columnsOpen &&
      !event.target.closest(".columns-popover") &&
      !event.target.closest("#columns-toggle")
    ) {
      runtime.columnsOpen = false;
      renderColumnPopover();
    }
  });

  window.addEventListener("resize", updateFilterPopoverPosition);
  window.addEventListener("scroll", updateFilterPopoverPosition, true);
  window.addEventListener("resize", updateColumnPopoverPosition);
  window.addEventListener("scroll", updateColumnPopoverPosition, true);
}

async function fetchText(url) {
  const response = await fetch(url, { cache: "default" });
  if (!response.ok) {
    throw new Error(`${url} returned ${response.status}`);
  }
  return response.text();
}

function buildTables(moviesCsv, sessionsCsv, imdbCsv) {
  const imdbById = new Map(
    imdbCsv.map((row) => [
      row.id,
      {
        imdb_id: row.imdb_id || "",
        watched: parseBoolean(row.watched),
        imdb_lists: parseList(row.imdb_lists),
      },
    ]),
  );
  const sessionCounts = sessionsCsv.reduce((counts, session) => {
    counts.set(session.id, (counts.get(session.id) || 0) + 1);
    return counts;
  }, new Map());

  const movieRows = moviesCsv.map((movie) => {
    const imdb = imdbById.get(movie.id) || {
      imdb_id: "",
      watched: false,
      imdb_lists: [],
    };

    return {
      id: movie.id,
      original_title: movie.original_title || "",
      title: movie.title || "",
      director: movie.director || "",
      cast: parseList(movie.cast),
      release_year: movie.release_year || "",
      runtime: movie.runtime || "",
      sessions: sessionCounts.get(movie.id) || 0,
      imdb_id: imdb.imdb_id,
      watched: imdb.watched,
      imdb_lists: imdb.imdb_lists,
      url: movie.url || "",
      imdb_url: imdb.imdb_id ? `https://www.imdb.com/title/${imdb.imdb_id}/` : "",
    };
  });

  const movieById = new Map(movieRows.map((movie) => [movie.id, movie]));
  const sessionTimesById = sessionsCsv.reduce((timesById, session) => {
    const time = parseDateValue(session.session);
    if (!Number.isNaN(time)) {
      const times = timesById.get(session.id) || [];
      times.push(time);
      timesById.set(session.id, times);
    }
    return timesById;
  }, new Map());
  const sessionRows = sessionsCsv.map((session) => {
    const movie = movieById.get(session.id) || {};
    const sessionTime = parseDateValue(session.session);
    const sessionTimes = sessionTimesById.get(session.id) || [];
    return {
      id: session.id,
      original_title: movie.original_title || "",
      title: movie.title || "",
      director: movie.director || "",
      cast: movie.cast || [],
      release_year: movie.release_year || "",
      runtime: movie.runtime || "",
      watched: movie.watched || false,
      session: formatSessionDateTime(session.session),
      session_sort: session.session || "",
      sessions_left: sessionTimes.filter((time) => time >= sessionTime).length,
      in_imdb_list: (movie.imdb_lists || []).length > 0,
      url: movie.url || "",
      imdb_url: "",
    };
  });

  return { sessions: sessionRows, movies: movieRows };
}

function render() {
  els.globalSearch.value = runtime.state.globalSearch;

  for (const tableName of Object.keys(els.panels)) {
    const active = runtime.state.activeTab === tableName;
    els.panels[tableName].classList.toggle("active", active);
  }

  for (const button of els.tabs) {
    const active = runtime.state.activeTab === button.dataset.tab;
    button.classList.toggle("active", active);
    button.setAttribute("aria-selected", String(active));
  }

  renderTable("sessions");
  renderTable("movies");
  renderFilterPopover();
  renderColumnPopover();
}

function renderTable(tableName) {
  const table = els.tables[tableName];
  const tableColumns = getVisibleColumns(tableName);
  const rows = getRows(tableName);
  runtime.filtered[tableName] = rows;
  els.counts[tableName].textContent = `(${rows.length}/${runtime.data[tableName].length})`;

  const header = tableColumns
    .map((column) => renderHeaderCell(tableName, column))
    .join("");

  const body =
    rows.length > 0
      ? rows.map((row) => renderRow(tableName, tableColumns, row)).join("")
      : `<tr><td class="empty-row" colspan="${Math.max(
          tableColumns.length,
          1,
        )}">No rows match the current filters.</td></tr>`;

  table.innerHTML = `<thead><tr>${header}</tr></thead><tbody>${body}</tbody>`;

  table.querySelectorAll(".sort-button").forEach((button) => {
    button.addEventListener("click", () => updateSort(tableName, button.dataset.key));
  });

  table.querySelectorAll(".filter-button").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const next = { tableName, key: button.dataset.key };
      runtime.columnsOpen = false;
      runtime.openFilter =
        runtime.openFilter &&
        runtime.openFilter.tableName === next.tableName &&
        runtime.openFilter.key === next.key
          ? null
          : next;
      renderFilterPopover();
    });
  });

  table.querySelectorAll("tbody tr[data-id]").forEach((row) => {
    row.addEventListener("dblclick", (event) => {
      if (event.target.closest("a, button, input, label")) {
        return;
      }
      if (window.getSelection?.().toString().trim()) {
        return;
      }
      jumpToRelatedTable(tableName, row.dataset.id);
    });
  });
}

function renderHeaderCell(tableName, column) {
  const tableState = runtime.state.tables[tableName];
  const sort = tableState.sort;
  const selected = tableState.filters[column.key] || [];
  const isSorted = sort.key === column.key;
  const mark = isSorted ? (sort.direction === "asc" ? "^" : "v") : "";
  const filterActive = selected.length > 0;
  const filteredClass = filterActive ? "filtered-column" : "";
  const filterLabel = filterActive
    ? `Filter ${column.label}: ${selected.length} selected`
    : `Filter ${column.label}`;

  const sortButton =
    column.sortable === false
      ? `<span class="sort-button">${escapeHtml(column.label)}</span>`
      : `<button class="sort-button" type="button" data-key="${escapeAttr(column.key)}">
          ${escapeHtml(column.label)} <span class="sort-mark">${mark}</span>
        </button>`;

  const filterButton =
    column.filterable === false
      ? ""
      : `<button class="filter-button ${filterActive ? "active" : ""}" type="button"
          data-table="${escapeAttr(tableName)}" data-key="${escapeAttr(column.key)}"
          aria-label="${escapeAttr(filterLabel)}" title="${escapeAttr(filterLabel)}"></button>
        ${filterActive ? `<span class="filter-badge">${selected.length}</span>` : ""}`;

  return `<th class="${filteredClass}"><div class="header-cell">${sortButton}${filterButton}</div></th>`;
}

function renderRow(tableName, tableColumns, row) {
  const cells = tableColumns
    .map((column) => `<td>${renderCell(tableName, column, row)}</td>`)
    .join("");
  return `<tr data-id="${escapeAttr(row.id)}" title="Double-click to show related rows">${cells}</tr>`;
}

function renderCell(tableName, column, row) {
  if (column.type === "link") {
    return renderIconLink(row.url, "Open Medeia page");
  }

  if (column.type === "links") {
    return `<span class="link-group">
      ${renderIconLink(row.url, "Open Medeia page")}
      ${renderIconLink(row.imdb_url, "Open IMDb page")}
    </span>`;
  }

  if (column.type === "boolean") {
    return row[column.key] ? "Yes" : "No";
  }

  if (column.list) {
    return escapeHtml((row[column.key] || []).join(", "));
  }

  return escapeHtml(row[column.key] || "");
}

function renderIconLink(url, label) {
  if (!url) {
    return "";
  }

  return `<a class="icon-link" href="${escapeAttr(url)}" target="_blank" rel="noopener noreferrer" title="${escapeAttr(label)}">
    <span class="sr-only">${escapeHtml(label)}</span>
  </a>`;
}

function getRows(tableName) {
  const tableState = runtime.state.tables[tableName];
  const search = normalize(runtime.state.globalSearch);
  const filtered = runtime.data[tableName].filter((row) => {
    if (search && !getSearchText(tableName, row).includes(search)) {
      return false;
    }
    return matchesColumnFilters(tableName, row);
  });

  return sortRows(tableName, filtered, tableState.sort);
}

function matchesColumnFilters(tableName, row) {
  const filters = runtime.state.tables[tableName].filters;
  const columnByKey = new Map(columns[tableName].map((column) => [column.key, column]));

  for (const [key, selected] of Object.entries(filters)) {
    if (!selected || selected.length === 0) {
      continue;
    }

    const column = columnByKey.get(key);
    const selectedSet = new Set(selected);

    if (column && column.list) {
      const values = getList(row[key]).map(getFilterValue);
      if (!values.some((value) => selectedSet.has(value))) {
        return false;
      }
      continue;
    }

    if (!selectedSet.has(getFilterValue(getDisplayValue(row, key, column)))) {
      return false;
    }
  }

  return true;
}

function sortRows(tableName, rows, sort) {
  const column = columns[tableName].find((item) => item.key === sort.key);
  if (!column || column.sortable === false) {
    return rows;
  }

  const direction = sort.direction === "desc" ? -1 : 1;
  return [...rows].sort((left, right) => {
    const result = compareValues(
      getSortValue(left, sort.key, column),
      getSortValue(right, sort.key, column),
      column.type,
    );
    return result * direction;
  });
}

function updateSort(tableName, key) {
  const sort = runtime.state.tables[tableName].sort;
  if (sort.key !== key) {
    runtime.state.tables[tableName].sort = { key, direction: "asc" };
  } else if (sort.direction === "asc") {
    runtime.state.tables[tableName].sort = { key, direction: "desc" };
  } else {
    runtime.state.tables[tableName].sort = { key: "id", direction: "asc" };
  }
  saveState();
  render();
}

function jumpToRelatedTable(currentTable, id) {
  const targetTable = currentTable === "sessions" ? "movies" : "sessions";
  const hasRelatedRows = runtime.data[targetTable].some(
    (row) => String(row.id) === String(id),
  );
  if (!hasRelatedRows) {
    const targetLabel = targetTable === "sessions" ? "sessions" : "movie";
    showToast(`No related ${targetLabel} found.`);
    return;
  }

  runtime.state.activeTab = targetTable;
  runtime.state.globalSearch = "";
  runtime.state.tables[targetTable].filters = { id: [String(id)] };
  runtime.openFilter = null;
  saveState();
  render();
}

function showToast(message) {
  if (!els.toast) {
    return;
  }

  clearTimeout(runtime.toastTimer);
  clearTimeout(runtime.toastHideTimer);
  els.toast.textContent = message;
  els.toast.hidden = false;

  requestAnimationFrame(() => {
    els.toast.classList.add("visible");
  });

  runtime.toastTimer = setTimeout(() => {
    els.toast.classList.remove("visible");
  }, 1600);

  runtime.toastHideTimer = setTimeout(() => {
    els.toast.hidden = true;
  }, 2100);
}

function renderFilterPopover() {
  const popover = els.popover;
  if (!runtime.openFilter) {
    popover.hidden = true;
    popover.innerHTML = "";
    return;
  }

  const { tableName, key } = runtime.openFilter;
  const button = getOpenFilterButton();
  const column = columns[tableName].find((item) => item.key === key);
  if (!button || !column) {
    popover.hidden = true;
    return;
  }
  const previousScrollTop =
    popover.querySelector(".filter-options")?.scrollTop || 0;

  const allOptions = getColumnOptions(tableName, column);
  const search = getFilterSearch(tableName, key);
  const visibleOptions = allOptions.filter((option) =>
    normalize(option.label).includes(normalize(search)),
  );
  const selected = new Set(runtime.state.tables[tableName].filters[key] || []);
  const selectedText =
    selected.size === 0 ? "All" : `${selected.size} selected`;

  popover.innerHTML = `
    <div class="filter-head">
      <div class="filter-title">
        <span>${escapeHtml(column.label)}</span>
        <span>${escapeHtml(selectedText)}</span>
      </div>
      <input type="search" data-role="filter-search" placeholder="Search this column" value="${escapeAttr(search)}">
      <div class="filter-actions">
        <button type="button" data-action="select-visible">Select visible</button>
        <button type="button" data-action="clear">Clear</button>
        <button type="button" data-action="done">Done</button>
      </div>
    </div>
    <div class="filter-options">
      ${
        visibleOptions.length
          ? visibleOptions
              .map(
                (option) => `
                  <label class="filter-option">
                    <input type="checkbox" value="${escapeAttr(option.value)}" ${
                  selected.has(option.value) ? "checked" : ""
                }>
                    <span>${escapeHtml(option.label)}</span>
                  </label>
                `,
              )
              .join("")
          : `<div class="filter-option">No options</div>`
      }
    </div>
  `;

  positionPopover(popover, button);
  popover.hidden = false;
  bindPopoverEvents(tableName, key, visibleOptions);
  const options = popover.querySelector(".filter-options");
  if (options) {
    options.scrollTop = previousScrollTop;
  }
}

function bindPopoverEvents(tableName, key, visibleOptions) {
  const popover = els.popover;
  popover.onclick = (event) => event.stopPropagation();

  const searchInput = popover.querySelector('[data-role="filter-search"]');
  searchInput.addEventListener("input", () => {
    setFilterSearch(tableName, key, searchInput.value);
    renderFilterPopover();
    requestAnimationFrame(() => {
      const nextInput = els.popover.querySelector('[data-role="filter-search"]');
      nextInput?.focus();
      nextInput?.setSelectionRange(nextInput.value.length, nextInput.value.length);
    });
  });

  popover.querySelectorAll('input[type="checkbox"]').forEach((checkbox) => {
    checkbox.addEventListener("change", () => {
      const selected = new Set(runtime.state.tables[tableName].filters[key] || []);
      if (checkbox.checked) {
        selected.add(checkbox.value);
      } else {
        selected.delete(checkbox.value);
      }
      setColumnFilter(tableName, key, [...selected]);
    });
  });

  popover.querySelector('[data-action="select-visible"]').addEventListener("click", () => {
    const selected = new Set(runtime.state.tables[tableName].filters[key] || []);
    for (const option of visibleOptions) {
      selected.add(option.value);
    }
    setColumnFilter(tableName, key, [...selected]);
  });

  popover.querySelector('[data-action="clear"]').addEventListener("click", () => {
    setColumnFilter(tableName, key, []);
  });

  popover.querySelector('[data-action="done"]').addEventListener("click", () => {
    runtime.openFilter = null;
    renderFilterPopover();
  });
}

function setColumnFilter(tableName, key, values) {
  const filters = runtime.state.tables[tableName].filters;
  if (values.length) {
    filters[key] = values.sort((a, b) => a.localeCompare(b));
  } else {
    delete filters[key];
  }
  saveState();
  render();
}

function renderColumnPopover() {
  const popover = els.columnsPopover;
  els.columnsToggle.setAttribute("aria-expanded", String(runtime.columnsOpen));

  if (!runtime.columnsOpen) {
    popover.hidden = true;
    popover.innerHTML = "";
    return;
  }

  popover.innerHTML = `
    <div class="columns-head">
      <strong>Visible columns</strong>
      <button type="button" data-action="done">Done</button>
    </div>
    <div class="columns-body">
      ${getColumnSelectorGroups()
        .map((group) => renderColumnSelectorGroup(group))
        .join("")}
    </div>
  `;

  positionColumnsPopover();
  popover.hidden = false;
  bindColumnPopoverEvents();
}

function getColumnSelectorGroups() {
  return [
    {
      label: "Shared columns",
      items: sharedColumnKeys.map((key) => ({
        scope: "shared",
        key,
        label: getSharedColumnLabel(key),
        checked: isSharedColumnVisible(key),
      })),
    },
    {
      label: "Sessions only",
      items: columns.sessions
        .filter((column) => !sharedColumnKeys.includes(column.key))
        .map((column) => ({
          scope: "table",
          tableName: "sessions",
          key: column.key,
          label: column.label,
          checked: getVisibleColumnKeys("sessions").includes(column.key),
        })),
    },
    {
      label: "Movies only",
      items: columns.movies
        .filter((column) => !sharedColumnKeys.includes(column.key))
        .map((column) => ({
          scope: "table",
          tableName: "movies",
          key: column.key,
          label: column.label,
          checked: getVisibleColumnKeys("movies").includes(column.key),
        })),
    },
  ];
}

function renderColumnSelectorGroup(group) {
  const selectedCount = group.items.filter((item) => item.checked).length;

  return `
    <section class="column-group">
      <div class="column-group-title">
        <span>${group.label}</span>
        <span>${selectedCount}/${group.items.length}</span>
      </div>
      <div class="column-options">
        ${group.items
          .map(
            (item) => `
              <label class="column-option">
                <input type="checkbox" data-scope="${item.scope}" ${
              item.tableName ? `data-table="${item.tableName}"` : ""
            } data-key="${escapeAttr(item.key)}" ${item.checked ? "checked" : ""}>
                <span>${escapeHtml(item.label)}</span>
              </label>
            `,
          )
          .join("")}
      </div>
    </section>
  `;
}

function bindColumnPopoverEvents() {
  const popover = els.columnsPopover;
  popover.onclick = (event) => event.stopPropagation();

  popover.querySelector('[data-action="done"]').addEventListener("click", () => {
    runtime.columnsOpen = false;
    renderColumnPopover();
  });

  popover.querySelectorAll('input[type="checkbox"]').forEach((checkbox) => {
    checkbox.addEventListener("change", () => {
      if (checkbox.dataset.scope === "shared") {
        setSharedColumnVisibility(checkbox.dataset.key, checkbox.checked);
      } else {
        setColumnVisibility(
          checkbox.dataset.table,
          checkbox.dataset.key,
          checkbox.checked,
        );
      }
    });
  });
}

function setSharedColumnVisibility(key, visible) {
  const targets = Object.keys(columns).filter((tableName) =>
    columns[tableName].some((column) => column.key === key),
  );

  if (!visible) {
    const wouldHideLastColumn = targets.some((tableName) => {
      const visibleKeys = new Set(getVisibleColumnKeys(tableName));
      return visibleKeys.size <= 1 && visibleKeys.has(key);
    });
    if (wouldHideLastColumn) {
      showToast("At least one column must stay visible.");
      renderColumnPopover();
      return;
    }
  }

  for (const tableName of targets) {
    const visibleKeys = new Set(getVisibleColumnKeys(tableName));
    if (visible) {
      visibleKeys.add(key);
    } else {
      visibleKeys.delete(key);
    }
    runtime.state.visibleColumns[tableName] = columns[tableName]
      .map((column) => column.key)
      .filter((columnKey) => visibleKeys.has(columnKey));
    delete runtime.state.tables[tableName].filters[key];
  }

  if (runtime.openFilter && runtime.openFilter.key === key) {
    runtime.openFilter = null;
  }

  saveState();
  render();
}

function setColumnVisibility(tableName, key, visible) {
  const visibleKeys = new Set(getVisibleColumnKeys(tableName));

  if (visible) {
    visibleKeys.add(key);
  } else if (visibleKeys.size <= 1 && visibleKeys.has(key)) {
    showToast("At least one column must stay visible.");
    renderColumnPopover();
    return;
  } else {
    visibleKeys.delete(key);
  }

  runtime.state.visibleColumns[tableName] = columns[tableName]
    .map((column) => column.key)
    .filter((columnKey) => visibleKeys.has(columnKey));

  delete runtime.state.tables[tableName].filters[key];
  if (
    runtime.openFilter &&
    runtime.openFilter.tableName === tableName &&
    runtime.openFilter.key === key
  ) {
    runtime.openFilter = null;
  }

  saveState();
  render();
}

function positionColumnsPopover() {
  const button = els.columnsToggle;
  const popover = els.columnsPopover;
  const rect = button.getBoundingClientRect();
  const width = Math.min(420, window.innerWidth - 24);
  const left = Math.min(Math.max(12, rect.right - width), window.innerWidth - width - 12);
  const top = Math.min(rect.bottom + 6, window.innerHeight - 24);
  popover.style.width = `${width}px`;
  popover.style.left = `${left}px`;
  popover.style.top = `${top}px`;
}

function updateColumnPopoverPosition(event) {
  if (!runtime.columnsOpen || els.columnsPopover.hidden) {
    return;
  }
  if (event?.target && els.columnsPopover.contains(event.target)) {
    return;
  }
  positionColumnsPopover();
}

function positionPopover(popover, button) {
  const rect = button.getBoundingClientRect();
  const width = Math.min(320, window.innerWidth - 24);
  const left = Math.min(Math.max(12, rect.left), window.innerWidth - width - 12);
  const top = Math.min(rect.bottom + 6, window.innerHeight - 24);
  popover.style.width = `${width}px`;
  popover.style.left = `${left}px`;
  popover.style.top = `${top}px`;
}

function updateFilterPopoverPosition(event) {
  if (!runtime.openFilter || els.popover.hidden) {
    return;
  }
  if (event?.target && els.popover.contains(event.target)) {
    return;
  }

  const button = getOpenFilterButton();
  if (!button) {
    runtime.openFilter = null;
    renderFilterPopover();
    return;
  }
  positionPopover(els.popover, button);
}

function getOpenFilterButton() {
  if (!runtime.openFilter) {
    return null;
  }
  const { tableName, key } = runtime.openFilter;
  return document.querySelector(
    `.filter-button[data-table="${cssEscape(tableName)}"][data-key="${cssEscape(key)}"]`,
  );
}

function getColumnOptions(tableName, column) {
  const values = new Map();
  for (const row of runtime.data[tableName]) {
    const rowValues = column.list
      ? getList(row[column.key])
      : [getDisplayValue(row, column.key, column)];

    for (const value of rowValues) {
      const label = String(value || "").trim();
      if (!label) {
        continue;
      }
      values.set(getFilterValue(label), label);
    }
  }

  return [...values.entries()]
    .map(([value, label]) => ({ value, label }))
    .sort((left, right) => compareOptionLabels(left.label, right.label, column.type));
}

function getSearchText(tableName, row) {
  return normalize(
    getVisibleColumns(tableName)
      .flatMap((column) => getSearchValues(row, column))
      .join(" "),
  );
}

function getSearchValues(row, column) {
  if (column.type === "links") {
    return [];
  }
  if (column.list) {
    return getList(row[column.key]);
  }
  return [getDisplayValue(row, column.key, column)];
}

function getDisplayValue(row, key, column) {
  if (column && column.type === "boolean") {
    return row[key] ? "Yes" : "No";
  }
  if (column && column.list) {
    return getList(row[key]).join(", ");
  }
  return row[key] ?? "";
}

function getSortValue(row, key, column) {
  if (key === "session") {
    return row.session_sort || row.session || "";
  }
  return getDisplayValue(row, key, column);
}

function compareValues(left, right, type) {
  if (type === "number") {
    return Number(left || 0) - Number(right || 0);
  }
  if (type === "date") {
    return parseDateValue(left) - parseDateValue(right);
  }
  if (type === "boolean") {
    return Number(left === "Yes") - Number(right === "Yes");
  }
  return String(left).localeCompare(String(right), undefined, {
    numeric: true,
    sensitivity: "base",
  });
}

function compareOptionLabels(left, right, type) {
  if (type === "number") {
    return Number(left) - Number(right);
  }
  return String(left).localeCompare(String(right), undefined, {
    numeric: true,
    sensitivity: "base",
  });
}

function parseDateValue(value) {
  return Date.parse(String(value || "").replace(" ", "T"));
}

function parseCsv(text) {
  const cleanText = text.replace(/^\uFEFF/, "");
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < cleanText.length; index += 1) {
    const char = cleanText[index];
    const next = cleanText[index + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        field += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      row.push(field);
      field = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && next === "\n") {
        index += 1;
      }
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }

    field += char;
  }

  if (field || row.length) {
    row.push(field);
    rows.push(row);
  }

  const [header, ...records] = rows;
  if (!header) {
    return [];
  }

  return records
    .filter((record) => record.some((value) => value.trim() !== ""))
    .map((record) =>
      Object.fromEntries(
        header.map((key, index) => [key.replace(/^\uFEFF/, ""), record[index] || ""]),
      ),
    );
}

function parseList(value) {
  if (!value || value === "[]") {
    return [];
  }

  try {
    const parsed = JSON.parse(value);
    if (Array.isArray(parsed)) {
      return parsed.map((item) => String(item));
    }
  } catch {
    // Fall back to parsing Python repr-style lists.
  }

  const matches = [];
  const pattern = /(['"])((?:\\.|(?!\1).)*)\1/g;
  let match = pattern.exec(value);
  while (match) {
    matches.push(match[2].replace(/\\(['"])/g, "$1"));
    match = pattern.exec(value);
  }
  return matches;
}

function parseBoolean(value) {
  return String(value).toLowerCase() === "true";
}

function formatSessionDateTime(value) {
  const match = String(value || "").match(
    /^(\d{4}-\d{2}-\d{2})[ T](\d{2}):(\d{2})/,
  );
  return match ? `${match[1]} ${match[2]}:${match[3]}` : String(value || "");
}

function getList(value) {
  return Array.isArray(value) ? value : value ? [value] : [];
}

function normalize(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function getFilterValue(value) {
  return String(value || "").trim();
}

function getDefaultVisibleColumns() {
  return Object.fromEntries(
    Object.entries(columns).map(([tableName, tableColumns]) => [
      tableName,
      tableColumns.map((column) => column.key),
    ]),
  );
}

function getSharedColumnLabel(key) {
  return (
    columns.sessions.find((column) => column.key === key)?.label ||
    columns.movies.find((column) => column.key === key)?.label ||
    key
  );
}

function isSharedColumnVisible(key) {
  return Object.keys(columns)
    .filter((tableName) => columns[tableName].some((column) => column.key === key))
    .every((tableName) => getVisibleColumnKeys(tableName).includes(key));
}

function normalizeSavedVisibleKeys(tableName, savedKeys) {
  if (!Array.isArray(savedKeys)) {
    return null;
  }

  return savedKeys.map((key) =>
    tableName === "sessions" && key === "url" ? "links" : key,
  );
}

function mergeVisibleColumns(savedVisibleColumns = {}, savedSchemaVersion = 0) {
  return Object.fromEntries(
    Object.keys(columns).map((tableName) => {
      const validKeys = columns[tableName].map((column) => column.key);
      const savedKeys = normalizeSavedVisibleKeys(
        tableName,
        savedVisibleColumns[tableName],
      );
      const baseKeys = savedKeys || validKeys;
      const visibleKeys = baseKeys.filter((key) => validKeys.includes(key));
      if (savedKeys && savedSchemaVersion !== COLUMN_SCHEMA_VERSION) {
        for (const key of validKeys) {
          if (!visibleKeys.includes(key)) {
            visibleKeys.push(key);
          }
        }
      }
      return [tableName, visibleKeys.length ? visibleKeys : validKeys];
    }),
  );
}

function syncSharedVisibleColumns(visibleColumns) {
  const next = JSON.parse(JSON.stringify(visibleColumns));
  for (const key of sharedColumnKeys) {
    const targetTables = Object.keys(columns).filter((tableName) =>
      columns[tableName].some((column) => column.key === key),
    );
    const visibleEverywhere = targetTables.every((tableName) =>
      next[tableName].includes(key),
    );
    for (const tableName of targetTables) {
      next[tableName] = next[tableName].filter((columnKey) => columnKey !== key);
      if (visibleEverywhere) {
        next[tableName].push(key);
      }
    }
  }
  return Object.fromEntries(
    Object.keys(columns).map((tableName) => [
      tableName,
      columns[tableName]
        .map((column) => column.key)
        .filter((key) => next[tableName].includes(key)),
    ]),
  );
}

function getVisibleColumnKeys(tableName) {
  return mergeVisibleColumns(
    runtime.state.visibleColumns,
    runtime.state.columnSchemaVersion,
  )[tableName];
}

function getVisibleColumns(tableName) {
  const visibleKeys = new Set(getVisibleColumnKeys(tableName));
  return columns[tableName].filter((column) => visibleKeys.has(column.key));
}

function loadState() {
  try {
    const saved = JSON.parse(localStorage.getItem(STORAGE_KEY));
    return mergeState(defaultState, saved || {});
  } catch {
    return cloneDefaultState();
  }
}

function cloneDefaultState() {
  return JSON.parse(JSON.stringify(defaultState));
}

function mergeState(base, saved) {
  const visibleColumns = syncSharedVisibleColumns(
    mergeVisibleColumns(saved.visibleColumns, saved.columnSchemaVersion),
  );

  return {
    activeTab: saved.activeTab || base.activeTab,
    globalSearch: saved.globalSearch || "",
    columnSchemaVersion: COLUMN_SCHEMA_VERSION,
    visibleColumns,
    tables: {
      sessions: {
        sort: mergeSort("sessions", saved.tables?.sessions?.sort),
        filters: sanitizeFilters(
          "sessions",
          saved.tables?.sessions?.filters || {},
          visibleColumns.sessions,
        ),
      },
      movies: {
        sort: mergeSort("movies", saved.tables?.movies?.sort),
        filters: sanitizeFilters(
          "movies",
          saved.tables?.movies?.filters || {},
          visibleColumns.movies,
        ),
      },
    },
  };
}

function mergeSort(tableName, savedSort) {
  const fallback = defaultState.tables[tableName].sort;
  const column = columns[tableName].find((item) => item.key === savedSort?.key);
  if (!column || column.sortable === false) {
    return fallback;
  }
  return {
    key: savedSort.key,
    direction: savedSort.direction === "desc" ? "desc" : "asc",
  };
}

function sanitizeFilters(tableName, filters, visibleKeys) {
  const visible = new Set(visibleKeys);
  const filterable = new Set(
    columns[tableName]
      .filter((column) => column.filterable !== false && visible.has(column.key))
      .map((column) => column.key),
  );

  return Object.fromEntries(
    Object.entries(filters).filter(
      ([key, values]) =>
        filterable.has(key) && Array.isArray(values) && values.length > 0,
    ),
  );
}

function saveState() {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(runtime.state));
  } catch {
    // Persistence is optional; the tables should continue to work without it.
  }
}

function getFilterSearch(tableName, key) {
  return runtime.filterSearch[`${tableName}:${key}`] || "";
}

function setFilterSearch(tableName, key, value) {
  runtime.filterSearch[`${tableName}:${key}`] = value;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function escapeAttr(value) {
  return escapeHtml(value);
}

function cssEscape(value) {
  if (window.CSS && CSS.escape) {
    return CSS.escape(value);
  }
  return String(value).replace(/"/g, '\\"');
}
