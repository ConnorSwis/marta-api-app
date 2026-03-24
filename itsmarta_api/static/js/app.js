(function () {
  "use strict";

  const BUS_MAP_DEFAULT_CENTER = [33.749, -84.388];
  const BUS_MAP_DEFAULT_ZOOM = 11;
  const BUS_MAP_POLL_MS = 10000;
  const ROUTE_COLOR_STORAGE_KEY = "marta-bus-route-colors-v1";
  const DEFAULT_VIEW = "arrivals";
  const RELIABILITY_SCOREBOARD_ENDPOINT =
    "/htmx/reliability/component/scoreboard";

  const busMapState = {
    map: null,
    markers: new Map(),
    routeColorCache: new Map(),
    routeColorCacheLoaded: false,
    pollTimer: null,
    endpoint: "",
    fitBoundsDone: false,
    loading: false,
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

    busMapState.markers.forEach((marker) => marker.remove());
    busMapState.markers.clear();
    if (busMapState.map) {
      busMapState.map.remove();
      busMapState.map = null;
    }

    busMapState.endpoint = "";
    busMapState.fitBoundsDone = false;
    busMapState.loading = false;
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

  function updateBusLegend(payload) {
    const legendList = document.querySelector("#buses-legend-list");
    const legendSummary = document.querySelector("#buses-legend-summary");
    if (!legendList || !legendSummary) {
      return;
    }

    const groupedByRoute = new Map();
    (payload.buses || []).forEach((bus) => {
      const routeKey = String(bus.route || "").trim() || "unknown";
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
      legendList.innerHTML =
        '<p class="buses-legend-empty">No active routes right now.</p>';
      return;
    }

    legendList.innerHTML = entries
      .map(
        (entry) => `
          <div class="buses-legend-item">
            <div class="buses-legend-route-cell">
              <span class="buses-legend-swatch" style="background:${entry.color};"></span>
              <span class="buses-legend-route">${escapeHtml(entry.route)}</span>
            </div>
            <span class="buses-legend-count">${entry.count}</span>
          </div>
        `,
      )
      .join("");
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

  function updateBusMarkers(payload, resetBounds) {
    if (!busMapState.map) {
      return;
    }

    const routeKeys = Array.from(
      new Set(
        (payload.buses || []).map(
          (bus) => String(bus.route || "").trim() || "unknown",
        ),
      ),
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

      const routeColor = getRouteColor(bus.route);
      const existingMarker = busMapState.markers.get(markerId);
      if (existingMarker) {
        existingMarker.setLatLng([bus.latitude, bus.longitude]);
        existingMarker.setPopupContent(buildBusPopupContent(bus));
        existingMarker.setStyle({ fillColor: routeColor });
        return;
      }

      const marker = window.L.circleMarker([bus.latitude, bus.longitude], {
        radius: 6,
        fillColor: routeColor,
        color: "#ffffff",
        weight: 1,
        fillOpacity: 0.9,
      });
      marker.bindPopup(buildBusPopupContent(bus));
      marker.addTo(busMapState.map);
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

  async function loadBusPositions(form, options = {}) {
    if (!busMapState.endpoint || !busMapState.map || busMapState.loading) {
      return;
    }

    const resetBounds = options.resetBounds === true;
    const formData = new FormData(form);
    const params = new URLSearchParams();
    const routeValue = String(formData.get("route") || "").trim();
    const vehicleValue = String(formData.get("vehicle_id") || "").trim();

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
    busMapState.map = window.L.map(mapElement, {
      zoomControl: true,
    });
    window.L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap contributors",
    }).addTo(busMapState.map);
    busMapState.map.setView(BUS_MAP_DEFAULT_CENTER, BUS_MAP_DEFAULT_ZOOM);
    window.requestAnimationFrame(
      () => busMapState.map && busMapState.map.invalidateSize(),
    );

    form.addEventListener("submit", (event) => {
      event.preventDefault();
      busMapState.fitBoundsDone = false;
      void loadBusPositions(form, { resetBounds: true });
    });

    const resetButton = form.querySelector("#buses-reset");
    if (resetButton) {
      resetButton.addEventListener("click", () => {
        form.reset();
        busMapState.fitBoundsDone = false;
        void loadBusPositions(form, { resetBounds: true });
      });
    }

    busMapState.pollTimer = window.setInterval(() => {
      void loadBusPositions(form);
    }, BUS_MAP_POLL_MS);

    void loadBusPositions(form, { resetBounds: true });
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
