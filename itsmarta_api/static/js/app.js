(function () {
  "use strict";

  const BUS_MAP_DEFAULT_CENTER = [33.749, -84.388];
  const BUS_MAP_DEFAULT_ZOOM = 11;
  const BUS_MAP_POLL_MS = 10000;
  const BUS_HISTORY_RANGE_PRESETS = [
    { minutes: 30, maxPoints: 160, sliderStep: 1 },
    { minutes: 60, maxPoints: 180, sliderStep: 1 },
    { minutes: 120, maxPoints: 200, sliderStep: 2 },
    { minutes: 240, maxPoints: 220, sliderStep: 2 },
    { minutes: 480, maxPoints: 220, sliderStep: 3 },
    { minutes: 720, maxPoints: 240, sliderStep: 4 },
    { minutes: 1440, maxPoints: 240, sliderStep: 6 },
  ];
  const BUS_HISTORY_DEFAULT_RANGE_MINUTES = 60;
  const BUS_HISTORY_BASE_DOWNSAMPLE_FACTOR = 2;
  const BUS_HISTORY_MAX_POINTS = 600;
  const BUS_HISTORY_AUTOPLAY_TOTAL_MS = 20000;
  const BUS_TRACE_WINDOW = 48;
  const BUS_TRACE_WINDOW_MINUTES = 20;
  const BUS_TRACE_MAX_PATHS = 400;
  const BUS_TRACE_MAX_POINTS_PER_PATH = 16;
  const BUS_TRACE_MAX_POINT_JUMP_METERS = 2500;
  const BUS_LIVE_TRACE_SNAPSHOT_LIMIT = 10;
  const BUS_LIVE_TRACE_LOOKBACK_HOURS = 2;
  const ROUTE_COLOR_STORAGE_KEY = "marta-bus-route-colors-v1";
  const DEFAULT_VIEW = "arrivals";
  const RELIABILITY_SCOREBOARD_ENDPOINT =
    "/htmx/reliability/component/scoreboard";

  const busMapState = {
    map: null,
    markers: new Map(),
    traceLayer: null,
    routeColorCache: new Map(),
    routeColorCacheLoaded: false,
    pollTimer: null,
    endpoint: "",
    historyEndpoint: "",
    mode: "live",
    historyTimeline: [],
    historyIndex: -1,
    historyRangeMinutes: BUS_HISTORY_DEFAULT_RANGE_MINUTES,
    historySliderStep: 1,
    historyLoading: false,
    historyTraceEnabled: true,
    historyAutoplayTimer: null,
    historyAutoplayTicking: false,
    liveTraceEntries: [],
    liveTraceRequestToken: 0,
    legendFocusRoute: "",
    fitBoundsDone: false,
    loading: false,
    renderToken: 0,
  };
  const reliabilityScoreboardScrollState = {
    left: 0,
    top: 0,
    hasValue: false,
  };

  function normalizePathname(pathname) {
    const value = String(pathname || "/").replace(/\/+$/, "");
    return value || "/";
  }

  function getViewFromPath(pathname) {
    const normalized = normalizePathname(pathname);
    if (normalized === "/" || normalized === "/arrivals") {
      return "arrivals";
    }
    if (normalized === "/schedules") {
      return "schedules";
    }
    if (normalized === "/buses") {
      return "buses";
    }
    if (normalized === "/reliability") {
      return "reliability";
    }
    return DEFAULT_VIEW;
  }

  function getPathForView(view) {
    if (view === "schedules") {
      return "/schedules";
    }
    if (view === "buses") {
      return "/buses";
    }
    if (view === "reliability") {
      return "/reliability";
    }
    return "/arrivals";
  }

  function getNavButton(view) {
    return document.querySelector(`.nav-tab[data-view-target='${view}']`);
  }

  function loadMainView(view, options = {}) {
    const pushHistory = options.pushHistory !== false;
    const replaceHistory = options.replaceHistory === true;
    const button = getNavButton(view);
    if (!button || !window.htmx) {
      return;
    }

    const body = document.querySelector("#body");
    const currentView = body && body.dataset ? body.dataset.view : "";
    if (currentView === view) {
      if (pushHistory) {
        const targetPath = getPathForView(view);
        const currentPath = normalizePathname(window.location.pathname);
        if (targetPath !== currentPath) {
          window.history[replaceHistory ? "replaceState" : "pushState"](
            { view },
            "",
            targetPath,
          );
        }
      }
      return;
    }

    setSingleActive(Array.from(document.querySelectorAll(".nav-tab")), button);
    window.htmx.ajax("GET", button.getAttribute("hx-get"), {
      target: "#body",
      swap: "outerHTML",
    });

    if (pushHistory) {
      const targetPath = getPathForView(view);
      window.history[replaceHistory ? "replaceState" : "pushState"](
        { view },
        "",
        targetPath,
      );
    }
  }

  function setSingleActive(elements, activeElement) {
    elements.forEach((element) => {
      const isActive = element === activeElement;

      if (isActive) {
        element.classList.add("is-active");
      } else {
        element.classList.remove("is-active");
      }

      if (element.hasAttribute("aria-selected")) {
        element.setAttribute("aria-selected", isActive ? "true" : "false");
      }
    });
  }

  function syncLineChipState(root) {
    root.querySelectorAll(".line-radio-group").forEach((group) => {
      group.querySelectorAll(".line-radio").forEach((radio) => {
        const label = group.querySelector(`label[for='${radio.id}']`);
        if (!label) {
          return;
        }

        if (radio.checked) {
          label.classList.add("is-active");
        } else {
          label.classList.remove("is-active");
        }
      });
    });
  }

  function initNavigation(root) {
    root.querySelectorAll(".nav-tab").forEach((button) => {
      if (button.dataset.bound === "true") {
        return;
      }

      button.dataset.bound = "true";
      button.addEventListener(
        "click",
        (event) => {
          event.preventDefault();
          event.stopImmediatePropagation();
          event.stopPropagation();
          const targetView = button.dataset.viewTarget || DEFAULT_VIEW;
          loadMainView(targetView, { pushHistory: true });
        },
        { capture: true },
      );
    });
  }

  function syncNavigationFromView(root) {
    const body = root.id === "body" ? root : root.querySelector("#body");
    if (!body || !body.dataset.view) {
      return;
    }

    const view = body.dataset.view;
    const matchingButton = document.querySelector(
      `.nav-tab[data-view-target='${view}']`,
    );
    if (!matchingButton) {
      return;
    }

    setSingleActive(
      Array.from(document.querySelectorAll(".nav-tab")),
      matchingButton,
    );
  }

  function initArrivalsFilters(root) {
    const form = root.querySelector("#arrivals-filters");
    if (!form || form.dataset.bound === "true") {
      return;
    }

    form.dataset.bound = "true";
    form.addEventListener("change", () => syncLineChipState(document));
    syncLineChipState(document);

    const clearButton = form.querySelector("#arrivals-clear");
    if (!clearButton) {
      return;
    }

    clearButton.addEventListener("click", () => {
      form.reset();
      syncLineChipState(document);
      form.dispatchEvent(new Event("change", { bubbles: true }));
    });
  }

  function initScheduleLineButtons(root) {
    root.querySelectorAll(".schedule-line-btn").forEach((button) => {
      if (button.dataset.bound === "true") {
        return;
      }

      button.dataset.bound = "true";
      button.addEventListener("click", () => {
        const group = Array.from(
          document.querySelectorAll(".schedule-line-btn"),
        );
        setSingleActive(group, button);
      });
    });
  }

  function initScheduleWidgets(root) {
    root.querySelectorAll(".schedule-widget").forEach((widget) => {
      if (widget.dataset.bound === "true") {
        return;
      }

      widget.dataset.bound = "true";
      let currentDirection = widget.dataset.defaultDirection || "";

      const dayButtons = Array.from(widget.querySelectorAll(".day-tab"));
      const dayPanels = Array.from(widget.querySelectorAll(".day-panel"));

      const activateDirection = (dayPanel, direction) => {
        const dirButtons = Array.from(
          dayPanel.querySelectorAll(".direction-tab"),
        );
        const dirPanels = Array.from(
          dayPanel.querySelectorAll(".direction-panel"),
        );

        let activeDirection = direction;
        if (
          !dirButtons.find((button) => button.dataset.direction === direction)
        ) {
          activeDirection = dirButtons[0]
            ? dirButtons[0].dataset.direction
            : "";
        }

        setSingleActive(
          dirButtons,
          dirButtons.find(
            (button) => button.dataset.direction === activeDirection,
          ),
        );

        dirPanels.forEach((panel) => {
          panel.classList.toggle(
            "is-active",
            panel.dataset.directionPanel === activeDirection,
          );
        });

        currentDirection = activeDirection;
      };

      const activateDay = (day) => {
        const activeDayButton = dayButtons.find(
          (button) => button.dataset.day === day,
        );
        const activeDayPanel = dayPanels.find(
          (panel) => panel.dataset.dayPanel === day,
        );

        if (!activeDayButton || !activeDayPanel) {
          return;
        }

        setSingleActive(dayButtons, activeDayButton);
        dayPanels.forEach((panel) => {
          panel.classList.toggle("is-active", panel === activeDayPanel);
        });

        activateDirection(activeDayPanel, currentDirection);
      };

      dayButtons.forEach((button) => {
        button.addEventListener("click", () => activateDay(button.dataset.day));
      });

      dayPanels.forEach((dayPanel) => {
        dayPanel.querySelectorAll(".direction-tab").forEach((button) => {
          button.addEventListener("click", () => {
            activateDirection(dayPanel, button.dataset.direction);
          });
        });
      });

      const firstDay =
        widget.dataset.defaultDay ||
        (dayButtons[0] && dayButtons[0].dataset.day) ||
        "";
      if (firstDay) {
        activateDay(firstDay);
      }
    });
  }

  function teardownBusMap() {
    if (busMapState.pollTimer) {
      window.clearInterval(busMapState.pollTimer);
      busMapState.pollTimer = null;
    }
    if (busMapState.historyAutoplayTimer) {
      window.clearInterval(busMapState.historyAutoplayTimer);
      busMapState.historyAutoplayTimer = null;
    }

    if (busMapState.traceLayer) {
      busMapState.traceLayer.remove();
      busMapState.traceLayer = null;
    }
    busMapState.markers.forEach((marker) => marker.remove());
    busMapState.markers.clear();
    if (busMapState.map) {
      busMapState.map.remove();
      busMapState.map = null;
    }

    busMapState.endpoint = "";
    busMapState.historyEndpoint = "";
    busMapState.mode = "live";
    busMapState.historyTimeline = [];
    busMapState.historyIndex = -1;
    busMapState.historyRangeMinutes = BUS_HISTORY_DEFAULT_RANGE_MINUTES;
    busMapState.historySliderStep = 1;
    busMapState.historyLoading = false;
    busMapState.historyTraceEnabled = true;
    busMapState.historyAutoplayTicking = false;
    busMapState.liveTraceEntries = [];
    busMapState.liveTraceRequestToken = 0;
    busMapState.legendFocusRoute = "";
    busMapState.fitBoundsDone = false;
    busMapState.loading = false;
    busMapState.renderToken += 1;
  }

  function escapeHtml(value) {
    return String(value || "").replace(/[&<>"']/g, (char) => {
      const entityMap = {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      };
      return entityMap[char] || char;
    });
  }

  function formatTimestamp(isoTimestamp) {
    if (!isoTimestamp) {
      return "Unknown";
    }

    const parsed = new Date(isoTimestamp);
    if (Number.isNaN(parsed.getTime())) {
      return "Unknown";
    }

    return parsed.toLocaleTimeString([], {
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  function formatDateTime(isoTimestamp) {
    if (!isoTimestamp) {
      return "Unknown";
    }

    const parsed = new Date(isoTimestamp);
    if (Number.isNaN(parsed.getTime())) {
      return "Unknown";
    }

    return parsed.toLocaleString([], {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  function buildBusPopupContent(bus) {
    const route = bus.route
      ? `Route ${escapeHtml(bus.route)}`
      : "Route unavailable";
    const speed =
      typeof bus.speed_mph === "number"
        ? `${bus.speed_mph.toFixed(1)} mph`
        : "Speed unknown";
    const status = bus.current_status
      ? escapeHtml(bus.current_status.replace(/_/g, " "))
      : "status unavailable";
    const updated = formatTimestamp(bus.last_updated);

    return `
      <div class="bus-popup">
        <span class="bus-popup-title">${route}</span>
        <p class="bus-popup-line">Vehicle ${escapeHtml(bus.vehicle_id || "unknown")}</p>
        <p class="bus-popup-line">${speed} | ${status}</p>
        <p class="bus-popup-line">Updated ${escapeHtml(updated)}</p>
      </div>
    `;
  }

  function updateBusMeta(payload, routeValue, vehicleValue) {
    const meta = document.querySelector("#buses-meta");
    if (!meta) {
      return;
    }

    const segments = [
      `${payload.count} buses loaded at ${payload.loaded_at || "unknown time"}`,
    ];
    if (routeValue) {
      segments.push(`Route ${routeValue}`);
    }
    if (vehicleValue) {
      segments.push(`Vehicle ${vehicleValue}`);
    }

    meta.textContent = segments.join(" | ");
  }

  function normalizeRouteKey(route) {
    return String(route || "").trim() || "unknown";
  }

  function applyMarkerFocusStyle(marker) {
    if (!marker) {
      return;
    }
    const markerRoute = marker.__routeKey || "unknown";
    const markerColor = marker.__routeColor || getRouteColor(markerRoute);
    const focusedRoute = busMapState.legendFocusRoute;
    const isFocused = !focusedRoute || focusedRoute === markerRoute;

    marker.setStyle({
      fillColor: isFocused ? markerColor : "#bdb8ad",
      color: isFocused ? "#0b0f0e" : "#d6d3d1",
      fillOpacity: isFocused ? 0.95 : 0.56,
      opacity: isFocused ? 0.95 : 0.5,
    });
  }

  function applyLegendFocusToMarkers() {
    busMapState.markers.forEach((marker) => applyMarkerFocusStyle(marker));
  }

  function applyLegendFocusToList() {
    const items = Array.from(document.querySelectorAll(".buses-legend-item"));
    const focusedRoute = busMapState.legendFocusRoute;
    items.forEach((item) => {
      const route = String(item.dataset.route || "").trim() || "unknown";
      const isActive = !!focusedRoute && route === focusedRoute;
      const isDim = !!focusedRoute && route !== focusedRoute;
      item.classList.toggle("is-active", isActive);
      item.classList.toggle("is-dim", isDim);
      item.setAttribute("aria-pressed", isActive ? "true" : "false");
    });
  }

  function setLegendFocusRoute(route) {
    const normalized = route ? normalizeRouteKey(route) : "";
    busMapState.legendFocusRoute =
      busMapState.legendFocusRoute === normalized ? "" : normalized;
    applyLegendFocusToList();
    applyLegendFocusToMarkers();
    if (busMapState.mode === "live") {
      renderLiveTrace();
      return;
    }
    const form = document.querySelector("#buses-controls");
    if (form && busMapState.historyIndex >= 0) {
      void renderHistorySnapshot(form, busMapState.historyIndex, {
        resetBounds: false,
      });
    }
  }

  function bindLegendInteractions() {
    const legendList = document.querySelector("#buses-legend-list");
    if (!legendList) {
      return;
    }
    Array.from(legendList.querySelectorAll(".buses-legend-item")).forEach(
      (item) => {
        item.addEventListener("click", () => {
          const route = String(item.dataset.route || "").trim();
          setLegendFocusRoute(route);
        });
      },
    );
  }

  function updateBusLegend(payload) {
    const legendList = document.querySelector("#buses-legend-list");
    const legendSummary = document.querySelector("#buses-legend-summary");
    if (!legendList || !legendSummary) {
      return;
    }

    const groupedByRoute = new Map();
    (payload.buses || []).forEach((bus) => {
      const routeKey = normalizeRouteKey(bus.route);
      groupedByRoute.set(routeKey, (groupedByRoute.get(routeKey) || 0) + 1);
    });

    const entries = Array.from(groupedByRoute.entries())
      .map(([route, count]) => ({
        route,
        count,
        color: getRouteColor(route),
      }))
      .sort((a, b) =>
        a.route.localeCompare(b.route, undefined, {
          numeric: true,
          sensitivity: "base",
        }),
      );

    legendSummary.textContent = `${entries.length} routes`;
    if (!entries.length) {
      busMapState.legendFocusRoute = "";
      legendList.innerHTML =
        '<p class="buses-legend-empty">No active routes right now.</p>';
      return;
    }

    if (
      busMapState.legendFocusRoute &&
      !entries.some((entry) => entry.route === busMapState.legendFocusRoute)
    ) {
      busMapState.legendFocusRoute = "";
    }

    legendList.innerHTML = entries
      .map(
        (entry) => `
          <button class="buses-legend-item" type="button" data-route="${escapeHtml(entry.route)}" aria-pressed="false">
            <div class="buses-legend-route-cell">
              <span class="buses-legend-swatch" style="background:${entry.color};"></span>
              <span class="buses-legend-route">${escapeHtml(entry.route)}</span>
            </div>
            <span class="buses-legend-count">${entry.count}</span>
          </button>
        `,
      )
      .join("");

    bindLegendInteractions();
    applyLegendFocusToList();
  }

  function updateBusError(errorText) {
    const errorNode = document.querySelector("#buses-error");
    if (!errorNode) {
      return;
    }

    if (errorText) {
      errorNode.textContent = errorText;
      errorNode.classList.remove("is-hidden");
      return;
    }

    errorNode.textContent = "";
    errorNode.classList.add("is-hidden");
  }

  function getCurrentBusFilters(form) {
    const formData = new FormData(form);
    return {
      routeValue: String(formData.get("route") || "").trim(),
      vehicleValue: String(formData.get("vehicle_id") || "").trim(),
    };
  }

  function getDefaultHistoryRangePreset() {
    return (
      BUS_HISTORY_RANGE_PRESETS.find(
        (preset) => preset.minutes === BUS_HISTORY_DEFAULT_RANGE_MINUTES,
      ) || BUS_HISTORY_RANGE_PRESETS[0]
    );
  }

  function getSelectedHistoryRangePreset() {
    const select = document.querySelector("#buses-history-lookback");
    const parsedMinutes = Number.parseInt(select ? select.value : "", 10);
    const selected = BUS_HISTORY_RANGE_PRESETS.find(
      (preset) => preset.minutes === parsedMinutes,
    );
    return selected || getDefaultHistoryRangePreset();
  }

  function syncSelectedHistoryRangeControl() {
    const select = document.querySelector("#buses-history-lookback");
    if (!select) {
      return;
    }
    const selected = BUS_HISTORY_RANGE_PRESETS.find(
      (preset) => preset.minutes === busMapState.historyRangeMinutes,
    );
    select.value = String(
      selected ? selected.minutes : BUS_HISTORY_DEFAULT_RANGE_MINUTES,
    );
  }

  function getHistoryMoveStep(count = busMapState.historyTimeline.length) {
    if (count <= 1) {
      return 1;
    }
    return Math.max(
      1,
      Math.min(busMapState.historySliderStep, Math.max(count - 1, 1)),
    );
  }

  function getTimelineSpanMinutes(snapshots) {
    if (!Array.isArray(snapshots) || snapshots.length < 2) {
      return 0;
    }
    const firstSnapshot = snapshots[0];
    const lastSnapshot = snapshots[snapshots.length - 1];
    const firstEpoch = new Date(firstSnapshot.captured_at_utc).getTime();
    const lastEpoch = new Date(lastSnapshot.captured_at_utc).getTime();
    if (!Number.isFinite(firstEpoch) || !Number.isFinite(lastEpoch)) {
      return 0;
    }
    const spanMs = Math.max(0, lastEpoch - firstEpoch);
    return spanMs / 60000;
  }

  function resolveHistorySliderStepForTimeline(snapshots) {
    const timelineCount = Array.isArray(snapshots) ? snapshots.length : 0;
    if (timelineCount <= 1) {
      return 1;
    }
    const spanMinutes = getTimelineSpanMinutes(snapshots);
    if (!Number.isFinite(spanMinutes) || spanMinutes <= 0) {
      return 1;
    }
    const matchedPreset = BUS_HISTORY_RANGE_PRESETS.find(
      (preset) => spanMinutes <= preset.minutes,
    );
    if (matchedPreset) {
      return Math.max(
        1,
        Number.parseInt(String(matchedPreset.sliderStep), 10) || 1,
      );
    }
    const maxPreset = BUS_HISTORY_RANGE_PRESETS[BUS_HISTORY_RANGE_PRESETS.length - 1];
    return Math.max(
      1,
      Number.parseInt(String(maxPreset.sliderStep), 10) || 1,
    );
  }

  function setBusMode(mode) {
    busMapState.mode = mode === "history" ? "history" : "live";
    if (busMapState.mode !== "history") {
      stopHistoryAutoplay();
    }
    const liveButton = document.querySelector("#buses-mode-live");
    const historyButton = document.querySelector("#buses-mode-history");
    const historyDetails = document.querySelector("#buses-history-details");
    if (liveButton) {
      liveButton.classList.toggle("is-active", busMapState.mode === "live");
    }
    if (historyButton) {
      historyButton.classList.toggle(
        "is-active",
        busMapState.mode === "history",
      );
    }
    if (historyDetails && busMapState.mode === "history") {
      historyDetails.open = true;
    }
  }

  function setHistoryControlsDisabled(disabled) {
    const slider = document.querySelector("#buses-history-slider");
    const prevButton = document.querySelector("#buses-history-prev");
    const nextButton = document.querySelector("#buses-history-next");
    if (slider) {
      slider.disabled = disabled;
    }
    if (prevButton) {
      prevButton.disabled = disabled;
    }
    if (nextButton) {
      nextButton.disabled = disabled;
    }
  }

  function updateHistoryLabel(text) {
    const label = document.querySelector("#buses-history-label");
    if (!label) {
      return;
    }
    label.textContent = text;
  }

  function getHistoryAutoplayIntervalMs(snapshotCount) {
    return Math.max(
      1,
      Math.round(BUS_HISTORY_AUTOPLAY_TOTAL_MS / Math.max(1, snapshotCount)),
    );
  }

  function updateHistorySpeedLabel() {
    const speedNode = document.querySelector("#buses-history-speed");
    if (!speedNode) {
      return;
    }
    if (!busMapState.historyAutoplayTimer) {
      speedNode.textContent = "";
      return;
    }
    const timeline = busMapState.historyTimeline || [];
    if (timeline.length < 2) {
      speedNode.textContent = "";
      return;
    }
    const firstEpoch = new Date(timeline[0].captured_at_utc).getTime();
    const lastEpoch = new Date(timeline[timeline.length - 1].captured_at_utc).getTime();
    if (!Number.isFinite(firstEpoch) || !Number.isFinite(lastEpoch)) {
      speedNode.textContent = "";
      return;
    }
    const realTimeSpanMs = Math.max(0, lastEpoch - firstEpoch);
    const autoplaySpanMs =
      Math.max(0, timeline.length - 1) * getHistoryAutoplayIntervalMs(timeline.length);
    if (realTimeSpanMs <= 0 || autoplaySpanMs <= 0) {
      speedNode.textContent = "";
      return;
    }
    const speedX = realTimeSpanMs / autoplaySpanMs;
    const speedText = speedX >= 100 ? Math.round(speedX).toString() : speedX.toFixed(1);
    speedNode.textContent = `(${speedText}x real-time)`;
  }

  function updateHistorySliderState() {
    const slider = document.querySelector("#buses-history-slider");
    const prevButton = document.querySelector("#buses-history-prev");
    const nextButton = document.querySelector("#buses-history-next");
    if (!slider) {
      return;
    }

    const count = busMapState.historyTimeline.length;
    const hasSnapshots = count > 0;
    if (!hasSnapshots) {
      slider.min = "0";
      slider.max = "0";
      slider.step = "1";
      slider.value = "0";
      updateHistoryRangeLabels("", "");
      updateHistorySpeedLabel();
      setHistoryControlsDisabled(true);
      updateHistoryAutoplayButtonState();
      return;
    }

    slider.min = "0";
    slider.max = String(count - 1);
    slider.step = String(getHistoryMoveStep(count));
    slider.value = String(
      Math.max(0, Math.min(busMapState.historyIndex, count - 1)),
    );
    slider.disabled = false;
    updateHistoryRangeLabels(
      busMapState.historyTimeline[0]
        ? busMapState.historyTimeline[0].captured_at_utc
        : "",
      busMapState.historyTimeline[count - 1]
        ? busMapState.historyTimeline[count - 1].captured_at_utc
        : "",
    );
    updateHistorySpeedLabel();
    updateHistoryAutoplayButtonState();

    if (prevButton) {
      prevButton.disabled = Number.parseInt(slider.value, 10) <= 0;
    }
    if (nextButton) {
      nextButton.disabled = Number.parseInt(slider.value, 10) >= count - 1;
    }
  }

  function updateHistoryRangeLabels(startTime, endTime) {
    const startNode = document.querySelector("#buses-history-start");
    const endNode = document.querySelector("#buses-history-end");
    if (!startNode || !endNode) {
      return;
    }

    startNode.textContent = startTime ? formatDateTime(startTime) : "--";
    endNode.textContent = endTime ? formatDateTime(endTime) : "--";
  }

  function updateHistoryAutoplayButtonState() {
    const autoplayButton = document.querySelector("#buses-history-autoplay");
    if (!autoplayButton) {
      return;
    }
    const hasSnapshots = (busMapState.historyTimeline || []).length > 0;
    const isPlaying = !!busMapState.historyAutoplayTimer;
    autoplayButton.disabled = !hasSnapshots;
    autoplayButton.textContent = isPlaying ? "Pause" : "Play";
    updateHistorySpeedLabel();
  }

  function ensureTraceLayer() {
    if (!busMapState.map) {
      return null;
    }
    if (!busMapState.traceLayer) {
      busMapState.traceLayer = window.L.layerGroup().addTo(busMapState.map);
    }
    return busMapState.traceLayer;
  }

  function clearTraceLayer() {
    if (!busMapState.traceLayer) {
      return;
    }
    busMapState.traceLayer.clearLayers();
  }

  function stopHistoryAutoplay() {
    if (busMapState.historyAutoplayTimer) {
      window.clearInterval(busMapState.historyAutoplayTimer);
      busMapState.historyAutoplayTimer = null;
    }
    busMapState.historyAutoplayTicking = false;
    updateHistoryAutoplayButtonState();
  }

  async function startHistoryAutoplay(form) {
    if (!form) {
      return;
    }
    const timeline = busMapState.historyTimeline || [];
    if (!timeline.length) {
      updateHistoryAutoplayButtonState();
      return;
    }
    const autoplayIntervalMs = getHistoryAutoplayIntervalMs(timeline.length);

    const startIndex =
      busMapState.historyIndex >= timeline.length - 1
        ? 0
        : Math.max(0, Math.min(busMapState.historyIndex, timeline.length - 1));

    stopHistoryAutoplay();
    await renderHistorySnapshot(form, startIndex, {
      resetBounds: false,
    });
    updateHistoryAutoplayButtonState();

    busMapState.historyAutoplayTimer = window.setInterval(async () => {
      if (busMapState.mode !== "history") {
        stopHistoryAutoplay();
        return;
      }
      if (busMapState.historyAutoplayTicking) {
        return;
      }

      const count = busMapState.historyTimeline.length;
      if (!count || busMapState.historyIndex >= count - 1) {
        stopHistoryAutoplay();
        return;
      }

      busMapState.historyAutoplayTicking = true;
      try {
        const nextIndex = Math.min(count - 1, busMapState.historyIndex + 1);
        await renderHistorySnapshot(form, nextIndex, { resetBounds: false });
      } finally {
        busMapState.historyAutoplayTicking = false;
      }
    }, autoplayIntervalMs);

    updateHistoryAutoplayButtonState();
  }

  function downsampleSnapshots(snapshots) {
    if (!Array.isArray(snapshots) || !snapshots.length) {
      return [];
    }

    const dynamicFactor = Math.ceil(snapshots.length / BUS_HISTORY_MAX_POINTS);
    const factor = Math.max(
      BUS_HISTORY_BASE_DOWNSAMPLE_FACTOR,
      dynamicFactor,
      1,
    );
    if (factor <= 1) {
      return snapshots.slice();
    }

    const sampled = [];
    for (let index = 0; index < snapshots.length; index += factor) {
      sampled.push(snapshots[index]);
    }
    const lastSnapshot = snapshots[snapshots.length - 1];
    if (sampled[sampled.length - 1] !== lastSnapshot) {
      sampled.push(lastSnapshot);
    }
    return sampled;
  }

  function buildTraceEntriesFromSnapshots(
    snapshots,
    maxPaths = BUS_TRACE_MAX_PATHS,
    options = {},
  ) {
    const allowedVehicleIds =
      options && options.allowedVehicleIds instanceof Set
        ? options.allowedVehicleIds
        : null;
    const maxPointJumpMeters =
      options && Number.isFinite(options.maxPointJumpMeters)
        ? Math.max(0, Number(options.maxPointJumpMeters))
        : BUS_TRACE_MAX_POINT_JUMP_METERS;
    const groupedPaths = new Map();

    (snapshots || []).forEach((snapshot) => {
      (snapshot.buses || []).forEach((bus) => {
        if (
          typeof bus.latitude !== "number" ||
          typeof bus.longitude !== "number"
        ) {
          return;
        }

        const vehicleId = String(bus.vehicle_id || "").trim();
        const entityId = String(bus.entity_id || "").trim();
        const vehicleKey = vehicleId || (entityId ? `entity:${entityId}` : "");
        if (!vehicleKey || (allowedVehicleIds && !allowedVehicleIds.has(vehicleKey))) {
          return;
        }

        const entry = groupedPaths.get(vehicleKey) || {
          route: normalizeRouteKey(bus.route),
          points: [],
        };
        const point = [bus.latitude, bus.longitude];
        const lastPoint = entry.points[entry.points.length - 1];
        if (!lastPoint || lastPoint[0] !== point[0] || lastPoint[1] !== point[1]) {
          if (
            lastPoint &&
            getDistanceMeters(lastPoint[0], lastPoint[1], point[0], point[1]) >
              maxPointJumpMeters
          ) {
            // Break discontinuous jumps to avoid drawing unrealistic cross-city segments.
            entry.points = [point];
            groupedPaths.set(vehicleKey, entry);
            return;
          }
          entry.points.push(point);
          if (entry.points.length > BUS_TRACE_MAX_POINTS_PER_PATH) {
            entry.points = entry.points.slice(-BUS_TRACE_MAX_POINTS_PER_PATH);
          }
        }
        if (!entry.route && bus.route) {
          entry.route = normalizeRouteKey(bus.route);
        }
        groupedPaths.set(vehicleKey, entry);
      });
    });

    return Array.from(groupedPaths.values())
      .filter((entry) => entry.points.length >= 2)
      .sort((a, b) => b.points.length - a.points.length)
      .slice(0, maxPaths);
  }

  function getDistanceMeters(lat1, lon1, lat2, lon2) {
    const toRadians = (degrees) => (degrees * Math.PI) / 180;
    const earthRadiusMeters = 6371000;
    const deltaLat = toRadians(lat2 - lat1);
    const deltaLon = toRadians(lon2 - lon1);
    const lat1Rad = toRadians(lat1);
    const lat2Rad = toRadians(lat2);
    const haversine =
      Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2) +
      Math.cos(lat1Rad) *
        Math.cos(lat2Rad) *
        Math.sin(deltaLon / 2) *
        Math.sin(deltaLon / 2);
    const c = 2 * Math.atan2(Math.sqrt(haversine), Math.sqrt(1 - haversine));
    return earthRadiusMeters * c;
  }

  async function loadLiveTraceFromSnapshots(form) {
    if (
      busMapState.mode !== "live" ||
      !busMapState.historyTraceEnabled ||
      !busMapState.historyEndpoint
    ) {
      busMapState.liveTraceEntries = [];
      clearTraceLayer();
      return;
    }

    const requestToken = busMapState.liveTraceRequestToken + 1;
    busMapState.liveTraceRequestToken = requestToken;
    const { routeValue, vehicleValue } = getCurrentBusFilters(form);
    const params = new URLSearchParams();
    params.set("limit", String(BUS_LIVE_TRACE_SNAPSHOT_LIMIT));
    params.set("since_hours", String(BUS_LIVE_TRACE_LOOKBACK_HOURS));
    if (routeValue) {
      params.set("route", routeValue);
    }
    if (vehicleValue) {
      params.set("vehicle_id", vehicleValue);
    }

    try {
      const response = await fetch(
        `${busMapState.historyEndpoint}?${params.toString()}`,
        {
          headers: { Accept: "application/json" },
        },
      );
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }
      const payload = await response.json();
      if (requestToken !== busMapState.liveTraceRequestToken) {
        return;
      }
      const snapshots = Array.isArray(payload.snapshots)
        ? payload.snapshots
        : [];
      busMapState.liveTraceEntries = buildTraceEntriesFromSnapshots(snapshots);
      if (busMapState.mode !== "live" || !busMapState.historyTraceEnabled) {
        return;
      }
      renderLiveTrace();
    } catch (_error) {
      if (requestToken !== busMapState.liveTraceRequestToken) {
        return;
      }
      busMapState.liveTraceEntries = [];
      if (busMapState.mode !== "live") {
        return;
      }
      clearTraceLayer();
    }
  }

  function renderLiveTrace() {
    if (busMapState.mode !== "live" || !busMapState.historyTraceEnabled) {
      return;
    }
    clearTraceLayer();
    const layer = ensureTraceLayer();
    if (!layer) {
      return;
    }

    const focusedRoute = busMapState.legendFocusRoute;
    const traces = busMapState.liveTraceEntries || [];

    traces.forEach((entry) => {
      const isFocused = !focusedRoute || entry.route === focusedRoute;
      const polyline = window.L.polyline(entry.points, {
        color: isFocused ? getRouteColor(entry.route) : "#c8c2b5",
        weight: 2.4,
        opacity: isFocused ? 0.58 : 0.28,
        smoothFactor: 1.0,
        interactive: false,
      });
      layer.addLayer(polyline);
    });
  }

  function filterSnapshotBuses(snapshotBuses, routeValue, vehicleValue) {
    const normalizedRoute = String(routeValue || "")
      .trim()
      .toLowerCase();
    const normalizedVehicle = String(vehicleValue || "")
      .trim()
      .toLowerCase();
    if (!normalizedRoute && !normalizedVehicle) {
      return snapshotBuses || [];
    }

    return (snapshotBuses || []).filter((bus) => {
      const route = String(bus.route || "")
        .trim()
        .toLowerCase();
      const vehicle = String(bus.vehicle_id || "")
        .trim()
        .toLowerCase();
      if (normalizedRoute && route !== normalizedRoute) {
        return false;
      }
      if (normalizedVehicle && vehicle !== normalizedVehicle) {
        return false;
      }
      return true;
    });
  }

  function updateBusMarkers(payload, resetBounds) {
    if (!busMapState.map) {
      return;
    }

    const routeKeys = Array.from(
      new Set((payload.buses || []).map((bus) => normalizeRouteKey(bus.route))),
    ).sort((a, b) =>
      a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" }),
    );
    routeKeys.forEach((routeKey) => getRouteColor(routeKey));

    const activeMarkerIds = new Set();
    const activePositions = [];

    (payload.buses || []).forEach((bus) => {
      if (
        typeof bus.latitude !== "number" ||
        typeof bus.longitude !== "number"
      ) {
        return;
      }

      const markerId = bus.vehicle_id || bus.entity_id;
      if (!markerId) {
        return;
      }

      activeMarkerIds.add(markerId);
      activePositions.push([bus.latitude, bus.longitude]);

      const routeKey = normalizeRouteKey(bus.route);
      const routeColor = getRouteColor(routeKey);
      const existingMarker = busMapState.markers.get(markerId);
      if (existingMarker) {
        existingMarker.setLatLng([bus.latitude, bus.longitude]);
        existingMarker.setPopupContent(buildBusPopupContent(bus));
        existingMarker.__routeKey = routeKey;
        existingMarker.__routeColor = routeColor;
        applyMarkerFocusStyle(existingMarker);
        return;
      }

      const marker = window.L.circleMarker([bus.latitude, bus.longitude], {
        radius: 7,
        fillColor: routeColor,
        color: "#0b0f0e",
        weight: 1.8,
        fillOpacity: 0.95,
        opacity: 0.95,
      });
      marker.__routeKey = routeKey;
      marker.__routeColor = routeColor;
      marker.bindPopup(buildBusPopupContent(bus));
      marker.addTo(busMapState.map);
      applyMarkerFocusStyle(marker);
      busMapState.markers.set(markerId, marker);
    });

    busMapState.markers.forEach((marker, markerId) => {
      if (!activeMarkerIds.has(markerId)) {
        marker.remove();
        busMapState.markers.delete(markerId);
      }
    });

    if (!activePositions.length) {
      busMapState.map.setView(BUS_MAP_DEFAULT_CENTER, BUS_MAP_DEFAULT_ZOOM);
      busMapState.fitBoundsDone = false;
      return;
    }

    if (resetBounds || !busMapState.fitBoundsDone) {
      busMapState.map.fitBounds(activePositions, {
        padding: [28, 28],
        maxZoom: 14,
      });
      busMapState.fitBoundsDone = true;
    }
  }

  function getRouteColor(route) {
    const key = String(route || "").trim() || "unknown";
    ensureRouteColorCacheLoaded();
    const cached = busMapState.routeColorCache.get(key);
    if (cached) {
      return cached;
    }

    // Deterministic route -> color mapping with contrast-aware candidate search.
    const seed = routeSeed(key);
    const saturationLevels = [88, 78, 70, 64];
    const lightnessLevels = [44, 52, 60, 38];
    const baseHue = (seed * 0.61803398875 * 360) % 360;

    let selectedColor = "";
    let firstAvailableColor = "";
    const minDistancePasses = [56, 46, 38, 30, 24, 18, 12];

    for (const minDistance of minDistancePasses) {
      for (let attempt = 0; attempt < 720; attempt += 1) {
        const hue = (baseHue + attempt * 137.50776405003785) % 360;
        const saturation =
          saturationLevels[(seed + attempt) % saturationLevels.length];
        const lightness =
          lightnessLevels[(seed + attempt * 3) % lightnessLevels.length];
        const candidate = hslToHex(hue, saturation, lightness);

        if (!firstAvailableColor && !colorAlreadyUsed(candidate)) {
          firstAvailableColor = candidate;
        }

        if (colorAlreadyUsed(candidate)) {
          continue;
        }
        if (isColorDistinct(candidate, minDistance)) {
          selectedColor = candidate;
          break;
        }
      }

      if (selectedColor) {
        break;
      }
    }

    if (!selectedColor) {
      selectedColor = firstAvailableColor || hslToHex(baseHue, 82, 48);
    }

    busMapState.routeColorCache.set(key, selectedColor);
    persistRouteColorCache();
    return selectedColor;
  }

  function ensureRouteColorCacheLoaded() {
    if (busMapState.routeColorCacheLoaded) {
      return;
    }

    busMapState.routeColorCacheLoaded = true;
    try {
      const raw = window.localStorage.getItem(ROUTE_COLOR_STORAGE_KEY);
      if (!raw) {
        return;
      }

      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") {
        return;
      }

      Object.entries(parsed).forEach(([route, color]) => {
        if (typeof route !== "string" || typeof color !== "string") {
          return;
        }
        if (!/^#[0-9a-f]{6}$/i.test(color)) {
          return;
        }
        busMapState.routeColorCache.set(route, color.toLowerCase());
      });
    } catch (_error) {
      // Ignore localStorage errors and continue with in-memory generation.
    }
  }

  function persistRouteColorCache() {
    try {
      const serialized = JSON.stringify(
        Object.fromEntries(busMapState.routeColorCache.entries()),
      );
      window.localStorage.setItem(ROUTE_COLOR_STORAGE_KEY, serialized);
    } catch (_error) {
      // Ignore localStorage failures.
    }
  }

  function colorAlreadyUsed(hexColor) {
    return Array.from(busMapState.routeColorCache.values()).includes(
      hexColor.toLowerCase(),
    );
  }

  function isColorDistinct(hexColor, minDistance) {
    const candidate = hexToRgb(hexColor);
    if (!candidate) {
      return false;
    }

    for (const assigned of busMapState.routeColorCache.values()) {
      const compared = hexToRgb(assigned);
      if (!compared) {
        continue;
      }

      const distance = Math.sqrt(
        (candidate.r - compared.r) ** 2 +
          (candidate.g - compared.g) ** 2 +
          (candidate.b - compared.b) ** 2,
      );
      if (distance < minDistance) {
        return false;
      }
    }

    return true;
  }

  function hexToRgb(hexColor) {
    const normalized = String(hexColor || "")
      .trim()
      .toLowerCase();
    if (!/^#[0-9a-f]{6}$/.test(normalized)) {
      return null;
    }

    return {
      r: Number.parseInt(normalized.slice(1, 3), 16),
      g: Number.parseInt(normalized.slice(3, 5), 16),
      b: Number.parseInt(normalized.slice(5, 7), 16),
    };
  }

  function routeSeed(routeKey) {
    if (/^[0-9]+$/.test(routeKey)) {
      const numeric = Number.parseInt(routeKey, 10);
      if (Number.isFinite(numeric) && numeric >= 0) {
        return numeric >>> 0;
      }
    }

    return hashRouteKey(routeKey);
  }

  function hashRouteKey(value) {
    // FNV-1a 32-bit hash for stable cross-session route hashing.
    let hash = 0x811c9dc5;
    for (let i = 0; i < value.length; i += 1) {
      hash ^= value.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193);
    }
    return hash >>> 0;
  }

  function hslToHex(h, s, l) {
    const saturation = s / 100;
    const lightness = l / 100;
    const chroma = (1 - Math.abs(2 * lightness - 1)) * saturation;
    const segment = h / 60;
    const x = chroma * (1 - Math.abs((segment % 2) - 1));
    let r = 0;
    let g = 0;
    let b = 0;

    if (segment >= 0 && segment < 1) {
      r = chroma;
      g = x;
    } else if (segment < 2) {
      r = x;
      g = chroma;
    } else if (segment < 3) {
      g = chroma;
      b = x;
    } else if (segment < 4) {
      g = x;
      b = chroma;
    } else if (segment < 5) {
      r = x;
      b = chroma;
    } else {
      r = chroma;
      b = x;
    }

    const match = lightness - chroma / 2;
    const toHex = (channel) => {
      const value = Math.round((channel + match) * 255)
        .toString(16)
        .padStart(2, "0");
      return value;
    };

    return `#${toHex(r)}${toHex(g)}${toHex(b)}`;
  }

  async function loadLiveBusPositions(form, options = {}) {
    if (!busMapState.endpoint || !busMapState.map || busMapState.loading) {
      return;
    }
    if (busMapState.mode !== "live") {
      return;
    }

    const resetBounds = options.resetBounds === true;
    const params = new URLSearchParams();
    const { routeValue, vehicleValue } = getCurrentBusFilters(form);

    if (routeValue) {
      params.set("route", routeValue);
    }
    if (vehicleValue) {
      params.set("vehicle_id", vehicleValue);
    }

    const endpointUrl = params.toString()
      ? `${busMapState.endpoint}?${params.toString()}`
      : busMapState.endpoint;

    busMapState.loading = true;
    try {
      const response = await fetch(endpointUrl, {
        headers: {
          Accept: "application/json",
        },
      });
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }

      const payload = await response.json();
      updateBusMeta(payload, routeValue, vehicleValue);
      updateBusError(payload.error || "");
      updateBusMarkers(payload, resetBounds);
      updateBusLegend(payload);
      void loadLiveTraceFromSnapshots(form);
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : "Could not load bus positions.";
      updateBusError(message);
    } finally {
      busMapState.loading = false;
    }
  }

  async function loadBusHistoryTimeline(form, options = {}) {
    if (!busMapState.historyEndpoint || busMapState.historyLoading) {
      return;
    }

    const preserveSelection = options.preserveSelection === true;
    const resetBounds = options.resetBounds === true;
    const previousSnapshot =
      busMapState.historyIndex >= 0 &&
      busMapState.historyIndex < busMapState.historyTimeline.length
        ? busMapState.historyTimeline[busMapState.historyIndex]
        : null;
    const previousSnapshotId = previousSnapshot ? previousSnapshot.id : null;
    const { routeValue, vehicleValue } = getCurrentBusFilters(form);
    const historyPreset = getSelectedHistoryRangePreset();
    busMapState.historyRangeMinutes = historyPreset.minutes;
    busMapState.historySliderStep = 1;

    const params = new URLSearchParams();
    params.set("limit", String(historyPreset.maxPoints));
    params.set("since_minutes", String(historyPreset.minutes));
    if (routeValue) {
      params.set("route", routeValue);
    }
    if (vehicleValue) {
      params.set("vehicle_id", vehicleValue);
    }
    const endpointUrl = `${busMapState.historyEndpoint}?${params.toString()}`;

    busMapState.historyLoading = true;
    updateHistoryLabel("Loading history snapshots...");
    try {
      const response = await fetch(endpointUrl, {
        headers: { Accept: "application/json" },
      });
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }

      const payload = await response.json();
      const rawSnapshots = Array.isArray(payload.snapshots)
        ? payload.snapshots
        : [];
      const snapshots = downsampleSnapshots(rawSnapshots);
      busMapState.historyTimeline = snapshots;
      busMapState.historySliderStep = resolveHistorySliderStepForTimeline(
        snapshots,
      );

      if (!snapshots.length) {
        busMapState.historyIndex = -1;
        stopHistoryAutoplay();
        updateHistorySliderState();
        clearTraceLayer();
        updateHistoryLabel(
          "No historical snapshots available for this filter.",
        );
        updateBusLegend({ buses: [] });
        updateBusMeta(
          { count: 0, loaded_at: "unknown time" },
          routeValue,
          vehicleValue,
        );
        updateBusMarkers({ buses: [] }, true);
        return;
      }

      if (preserveSelection && previousSnapshotId) {
        const preservedIndex = snapshots.findIndex(
          (snapshot) => snapshot.id === previousSnapshotId,
        );
        if (preservedIndex >= 0) {
          busMapState.historyIndex = preservedIndex;
        } else if (previousSnapshot && previousSnapshot.captured_at_utc) {
          const previousTime = new Date(
            previousSnapshot.captured_at_utc,
          ).getTime();
          let nearestIndex = snapshots.length - 1;
          let nearestDistance = Number.POSITIVE_INFINITY;
          snapshots.forEach((snapshot, index) => {
            const snapshotTime = new Date(snapshot.captured_at_utc).getTime();
            if (
              !Number.isFinite(snapshotTime) ||
              !Number.isFinite(previousTime)
            ) {
              return;
            }
            const distance = Math.abs(snapshotTime - previousTime);
            if (distance < nearestDistance) {
              nearestDistance = distance;
              nearestIndex = index;
            }
          });
          busMapState.historyIndex = nearestIndex;
        } else {
          busMapState.historyIndex = snapshots.length - 1;
        }
      } else {
        busMapState.historyIndex = snapshots.length - 1;
      }
      updateHistorySliderState();
      await renderHistorySnapshot(form, busMapState.historyIndex, {
        resetBounds,
      });
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : "Could not load bus history snapshots.";
      updateBusError(message);
      updateHistoryLabel("Failed to load history snapshots.");
    } finally {
      busMapState.historyLoading = false;
    }
  }

  async function renderHistoryTrace(form, snapshotIndex, renderToken) {
    clearTraceLayer();
    if (busMapState.mode !== "history" || !busMapState.historyTraceEnabled) {
      return;
    }
    const layer = ensureTraceLayer();
    if (!layer) {
      return;
    }
    const timeline = busMapState.historyTimeline || [];
    if (
      !timeline.length ||
      snapshotIndex < 0 ||
      snapshotIndex >= timeline.length
    ) {
      return;
    }

    const endSnapshot = timeline[snapshotIndex];
    const endTime = new Date(endSnapshot.captured_at_utc).getTime();
    const maxAgeMs = BUS_TRACE_WINDOW_MINUTES * 60 * 1000;
    let startIndex = Math.max(0, snapshotIndex - BUS_TRACE_WINDOW + 1);
    if (Number.isFinite(endTime)) {
      for (let index = snapshotIndex; index >= 0; index -= 1) {
        const snapshotTime = new Date(timeline[index].captured_at_utc).getTime();
        if (!Number.isFinite(snapshotTime)) {
          continue;
        }
        if (endTime - snapshotTime <= maxAgeMs) {
          startIndex = index;
          continue;
        }
        break;
      }
    }
    const snapshotsSlice = timeline.slice(startIndex, snapshotIndex + 1);
    const { routeValue, vehicleValue } = getCurrentBusFilters(form);
    const visibleBuses = filterSnapshotBuses(
      endSnapshot.buses || [],
      routeValue,
      vehicleValue,
    );
    const allowedVehicleIds = new Set(
      visibleBuses
        .map((bus) =>
          String(bus.vehicle_id || "").trim() ||
          (String(bus.entity_id || "").trim()
            ? `entity:${String(bus.entity_id || "").trim()}`
            : ""),
        )
        .filter(Boolean),
    );
    const traces = buildTraceEntriesFromSnapshots(
      snapshotsSlice,
      BUS_TRACE_MAX_PATHS,
      {
        allowedVehicleIds,
      },
    );
    const focusedRoute = busMapState.legendFocusRoute;

    traces.forEach((entry) => {
      if (renderToken !== busMapState.renderToken) {
        return;
      }
      const normalizedRoute = normalizeRouteKey(entry.route);
      const isFocused = !focusedRoute || focusedRoute === normalizedRoute;
      const polyline = window.L.polyline(entry.points, {
        color: isFocused ? getRouteColor(normalizedRoute) : "#c8c2b5",
        weight: 2.8,
        opacity: isFocused ? 0.64 : 0.3,
        smoothFactor: 1.0,
        interactive: false,
      });
      layer.addLayer(polyline);
    });
  }

  async function renderHistorySnapshot(form, snapshotIndex, options = {}) {
    const resetBounds = options.resetBounds === true;
    const timeline = busMapState.historyTimeline || [];
    if (!timeline.length) {
      return;
    }

    const resolvedIndex = Math.max(
      0,
      Math.min(snapshotIndex, timeline.length - 1),
    );
    busMapState.historyIndex = resolvedIndex;
    updateHistorySliderState();

    const snapshot = timeline[resolvedIndex];
    const { routeValue, vehicleValue } = getCurrentBusFilters(form);
    const filteredBuses = filterSnapshotBuses(
      snapshot.buses || [],
      routeValue,
      vehicleValue,
    );
    const payload = {
      buses: filteredBuses,
      count: filteredBuses.length,
      loaded_at: formatDateTime(snapshot.captured_at_utc),
      error: null,
    };

    updateBusMeta(payload, routeValue, vehicleValue);
    updateBusError("");
    updateBusMarkers(payload, resetBounds);
    updateBusLegend(payload);
    updateHistoryLabel(
      `Snapshot ${resolvedIndex + 1}/${timeline.length} • ${filteredBuses.length} buses • ${formatDateTime(snapshot.captured_at_utc)}`,
    );

    busMapState.renderToken += 1;
    const renderToken = busMapState.renderToken;
    await renderHistoryTrace(form, resolvedIndex, renderToken);
  }

  function startLivePolling(form) {
    if (busMapState.pollTimer) {
      window.clearInterval(busMapState.pollTimer);
      busMapState.pollTimer = null;
    }
    busMapState.pollTimer = window.setInterval(() => {
      void loadLiveBusPositions(form);
    }, BUS_MAP_POLL_MS);
  }

  function stopLivePolling() {
    if (!busMapState.pollTimer) {
      return;
    }
    window.clearInterval(busMapState.pollTimer);
    busMapState.pollTimer = null;
  }

  function initBusMap() {
    const form = document.querySelector("#buses-controls");
    const mapElement = document.querySelector("#bus-map");
    if (!form || !mapElement) {
      teardownBusMap();
      return;
    }

    if (form.dataset.bound === "true") {
      return;
    }

    if (!window.L) {
      updateBusError("Map library failed to load. Refresh and try again.");
      return;
    }

    form.dataset.bound = "true";
    teardownBusMap();
    busMapState.endpoint = form.dataset.endpoint || "";
    busMapState.historyEndpoint = form.dataset.historyEndpoint || "";
    busMapState.map = window.L.map(mapElement, {
      zoomControl: true,
    });
    window.L.tileLayer(
      "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
      {
        maxZoom: 20,
        subdomains: "abcd",
        attribution:
          '&copy; OpenStreetMap contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      },
    ).addTo(busMapState.map);
    busMapState.map.setView(BUS_MAP_DEFAULT_CENTER, BUS_MAP_DEFAULT_ZOOM);
    window.requestAnimationFrame(
      () => busMapState.map && busMapState.map.invalidateSize(),
    );
    busMapState.historyRangeMinutes = getSelectedHistoryRangePreset().minutes;
    syncSelectedHistoryRangeControl();
    setBusMode("live");
    updateHistoryLabel("Live mode active");
    setHistoryControlsDisabled(true);

    form.addEventListener("submit", (event) => {
      event.preventDefault();
      busMapState.fitBoundsDone = false;
      if (busMapState.mode === "history") {
        void loadBusHistoryTimeline(form, {
          resetBounds: true,
          preserveSelection: true,
        });
        return;
      }
      void loadLiveBusPositions(form, { resetBounds: true });
    });

    const resetButton = form.querySelector("#buses-reset");
    if (resetButton) {
      resetButton.addEventListener("click", () => {
        form.reset();
        busMapState.fitBoundsDone = false;
        if (busMapState.mode === "history") {
          void loadBusHistoryTimeline(form, {
            resetBounds: true,
            preserveSelection: false,
          });
          return;
        }
        void loadLiveBusPositions(form, { resetBounds: true });
      });
    }

    const liveModeButton = document.querySelector("#buses-mode-live");
    if (liveModeButton) {
      liveModeButton.addEventListener("click", () => {
        setBusMode("live");
        stopLivePolling();
        busMapState.fitBoundsDone = false;
        setHistoryControlsDisabled(true);
        startLivePolling(form);
        void loadLiveBusPositions(form, { resetBounds: true });
      });
    }

    const historyModeButton = document.querySelector("#buses-mode-history");
    if (historyModeButton) {
      historyModeButton.addEventListener("click", () => {
        setBusMode("history");
        stopHistoryAutoplay();
        stopLivePolling();
        clearTraceLayer();
        busMapState.fitBoundsDone = false;
        void loadBusHistoryTimeline(form, {
          resetBounds: true,
          preserveSelection: true,
        });
      });
    }

    const historyRangeSelect = document.querySelector("#buses-history-lookback");
    if (historyRangeSelect) {
      historyRangeSelect.addEventListener("change", () => {
        const selectedPreset = getSelectedHistoryRangePreset();
        busMapState.historyRangeMinutes = selectedPreset.minutes;
        busMapState.historySliderStep = 1;
        stopHistoryAutoplay();
        if (busMapState.mode !== "history") {
          return;
        }
        void loadBusHistoryTimeline(form, {
          resetBounds: false,
          preserveSelection: true,
        });
      });
    }

    const slider = document.querySelector("#buses-history-slider");
    if (slider) {
      slider.addEventListener("input", () => {
        if (busMapState.mode !== "history") {
          return;
        }
        stopHistoryAutoplay();
        const targetIndex = Number.parseInt(slider.value, 10);
        if (!Number.isFinite(targetIndex)) {
          return;
        }
        void renderHistorySnapshot(form, targetIndex, { resetBounds: false });
      });
    }

    const prevButton = document.querySelector("#buses-history-prev");
    if (prevButton) {
      prevButton.addEventListener("click", () => {
        if (busMapState.mode !== "history") {
          return;
        }
        stopHistoryAutoplay();
        const nextIndex = Math.max(
          0,
          busMapState.historyIndex - getHistoryMoveStep(),
        );
        void renderHistorySnapshot(form, nextIndex, { resetBounds: false });
      });
    }

    const nextButton = document.querySelector("#buses-history-next");
    if (nextButton) {
      nextButton.addEventListener("click", () => {
        if (busMapState.mode !== "history") {
          return;
        }
        stopHistoryAutoplay();
        const nextIndex = Math.min(
          busMapState.historyTimeline.length - 1,
          busMapState.historyIndex + getHistoryMoveStep(),
        );
        void renderHistorySnapshot(form, nextIndex, { resetBounds: false });
      });
    }

    const autoplayButton = document.querySelector("#buses-history-autoplay");
    if (autoplayButton) {
      autoplayButton.addEventListener("click", () => {
        if (busMapState.mode !== "history") {
          return;
        }
        if (busMapState.historyAutoplayTimer) {
          stopHistoryAutoplay();
          return;
        }
        void startHistoryAutoplay(form);
      });
    }

    const traceToggle = document.querySelector("#buses-trace-toggle");
    if (traceToggle) {
      busMapState.historyTraceEnabled = traceToggle.checked === true;
      traceToggle.addEventListener("change", () => {
        busMapState.historyTraceEnabled = traceToggle.checked === true;
        if (busMapState.mode === "history") {
          void renderHistorySnapshot(form, busMapState.historyIndex, {
            resetBounds: false,
          });
          return;
        }
        if (busMapState.historyTraceEnabled) {
          void loadLiveTraceFromSnapshots(form);
          return;
        }
        clearTraceLayer();
      });
    }

    const refreshHistoryButton = document.querySelector(
      "#buses-history-refresh",
    );
    if (refreshHistoryButton) {
      refreshHistoryButton.addEventListener("click", () => {
        stopHistoryAutoplay();
        if (busMapState.mode === "history") {
          void loadBusHistoryTimeline(form, {
            resetBounds: false,
            preserveSelection: true,
          });
          return;
        }
      });
    }

    startLivePolling(form);
    void loadLiveBusPositions(form, { resetBounds: true });
    updateHistoryAutoplayButtonState();
  }

  function initialize(root) {
    initNavigation(document);
    syncNavigationFromView(root);
    initArrivalsFilters(root);
    initScheduleLineButtons(root);
    initScheduleWidgets(root);
    initBusMap();
  }

  function isReliabilityScoreboardRequest(event) {
    const requestPath =
      (event &&
        event.detail &&
        event.detail.pathInfo &&
        event.detail.pathInfo.requestPath) ||
      "";
    if (requestPath.includes(RELIABILITY_SCOREBOARD_ENDPOINT)) {
      return true;
    }

    const target = event && event.detail ? event.detail.target : null;
    return !!(target && target.id === "reliability-scoreboard");
  }

  function captureReliabilityScoreboardScroll() {
    const tableWrap = document.querySelector(
      "#reliability-scoreboard .scoreboard-table-wrap",
    );
    if (!tableWrap) {
      reliabilityScoreboardScrollState.hasValue = false;
      return;
    }

    reliabilityScoreboardScrollState.left = tableWrap.scrollLeft;
    reliabilityScoreboardScrollState.top = tableWrap.scrollTop;
    reliabilityScoreboardScrollState.hasValue = true;
  }

  function restoreReliabilityScoreboardScroll() {
    if (!reliabilityScoreboardScrollState.hasValue) {
      return;
    }

    const tableWrap = document.querySelector(
      "#reliability-scoreboard .scoreboard-table-wrap",
    );
    if (!tableWrap) {
      return;
    }

    tableWrap.scrollLeft = reliabilityScoreboardScrollState.left;
    tableWrap.scrollTop = reliabilityScoreboardScrollState.top;
  }

  document.addEventListener("DOMContentLoaded", () => {
    initialize(document);
    const requestedView = getViewFromPath(window.location.pathname);
    loadMainView(requestedView, { pushHistory: false, replaceHistory: true });
  });

  document.body.addEventListener("htmx:afterSwap", (event) => {
    if (isReliabilityScoreboardRequest(event)) {
      restoreReliabilityScoreboardScroll();
    }
    initialize(event.target);
  });

  document.body.addEventListener("htmx:beforeRequest", (event) => {
    if (isReliabilityScoreboardRequest(event)) {
      captureReliabilityScoreboardScroll();
    }
  });

  window.addEventListener("beforeunload", () => {
    teardownBusMap();
  });

  window.addEventListener("popstate", () => {
    const view = getViewFromPath(window.location.pathname);
    loadMainView(view, { pushHistory: false });
  });
})();
