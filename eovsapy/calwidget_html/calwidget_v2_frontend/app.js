(function () {
  const html = htm.bind(React.createElement);
  const useEffect = React.useEffect;
  const useRef = React.useRef;
  const useState = React.useState;

  const OVERVIEW_SECTIONS = [
    {
      id: "sum_amp",
      label: "Sum X & Y Amplitude",
      showLegend: false,
      panelHeight: 116,
    },
    {
      id: "sum_pha",
      label: "Sum X & Y Phase [rad]",
      showLegend: false,
      panelHeight: 116,
    },
    {
      id: "inband_fit",
      label: "Inband Fit",
      showLegend: true,
      panelHeight: 138,
    },
    {
      id: "inband_relative_phase",
      label: "Relative Phase + Fit",
      showLegend: true,
      panelHeight: 138,
    },
    {
      id: "inband_residual_phase_band",
      label: "Per-Band Residual Phase",
      showLegend: true,
      panelHeight: 138,
    },
    {
      id: "inband_residual_delay_band",
      label: "Residual Delay Per Band",
      showLegend: true,
      panelHeight: 126,
    },
  ];
  const TIME_FLAG_SCOPES = [
    { id: "selected", label: "Selected" },
    { id: "this_ant", label: "This Ant" },
    { id: "this_band", label: "This Band" },
    { id: "higher_bands", label: "Higher Bands" },
    { id: "all", label: "All" },
  ];
  const INBAND_SCOPE_OPTIONS = [
    { id: "selected", label: "Selected" },
    { id: "all", label: "All" },
  ];

  const COLOR_X = "#1f77b4";
  const COLOR_Y = "#ff7f0e";
  const PANEL_GRID_PANEL_WIDTH = 200;
  const TIME_FLAG_MIN_DRAG_PX = 6;
  const ACTION_PROGRESS = {
    refcal: {
      label: "Analyzing Refcal",
      success: "Refcal analysis complete",
      paceMs: 18000,
      waitForPlots: true,
      stages: [
        "Opening scan and averaging channels",
        "Applying legacy calibration corrections",
        "Fitting in-band delay solution",
        "Refreshing browser products",
      ],
    },
    phacal: {
      label: "Analyzing Phacal",
      success: "Phacal analysis complete",
      paceMs: 22000,
      waitForPlots: true,
      stages: [
        "Opening scan and aligning reference data",
        "Applying refcal and drift corrections",
        "Solving phase differences and multiband delay",
        "Refreshing browser products",
      ],
    },
    combine_refcal: {
      label: "Combining Refcals",
      success: "Refcal combination complete",
      paceMs: 12000,
      waitForPlots: true,
      stages: [
        "Loading selected refcals",
        "Merging compatible calibration products",
        "Updating combined scan state",
        "Refreshing browser products",
      ],
    },
    time_flag: {
      label: "Updating Time Flags",
      success: "Time-flag update complete",
      paceMs: 9000,
      waitForPlots: true,
      stages: [
        "Applying time-flag interval",
        "Re-averaging masked channels",
        "Refreshing plot products",
        "Updating browser display",
      ],
    },
    inband_mask: {
      label: "Applying In-Band Mask",
      success: "In-band mask applied",
      paceMs: 7000,
      waitForPlots: false,
      stages: [
        "Applying staged band mask",
        "Recomputing active in-band delay",
        "Refreshing relative-phase diagnostics",
        "Updating display panels",
      ],
    },
    active_delay: {
      label: "Updating In-Band Delay",
      success: "In-band delay updated",
      paceMs: 8000,
      waitForPlots: false,
      stages: [
        "Updating active delay values",
        "Recomputing corrected channels",
        "Refreshing affected diagnostics",
        "Updating display panels",
      ],
    },
    relative_delay: {
      label: "Updating Relative-Phase Fit",
      success: "Relative-phase fit updated",
      paceMs: 6000,
      waitForPlots: false,
      stages: [
        "Updating manual fit-delay values",
        "Recomputing fit overlays",
        "Refreshing residual diagnostics",
        "Updating display panels",
      ],
    },
    save_sql: {
      label: "Saving SQL",
      success: "Saved scan to SQL",
      paceMs: 7000,
      waitForPlots: false,
      stages: [
        "Preparing calibrated scan product",
        "Writing calibration arrays to SQL",
        "Refreshing saved-state metadata",
        "Updating browser state",
      ],
    },
    save_npz: {
      label: "Saving NPZ",
      success: "Daily NPZ bundle saved",
      paceMs: 7000,
      waitForPlots: false,
      stages: [
        "Collecting tuned scan products",
        "Building model-phase export arrays",
        "Writing daily bundle under /common/webplots/phasecal",
        "Updating browser state",
      ],
    },
  };

  async function jsonFetch(url, options) {
    const response = await fetch(url, options || {});
    const text = await response.text();
    let data = {};
    if (text) {
      try {
        data = JSON.parse(text);
      } catch (_err) {
        if (!response.ok) {
          throw new Error(text || response.statusText || "Request failed");
        }
        throw new Error("Invalid JSON response.");
      }
    }
    if (!response.ok) {
      throw new Error((data && data.detail) || text || response.statusText || "Request failed");
    }
    return data;
  }

  function todayIso() {
    return new Date().toISOString().slice(0, 10);
  }

  function trimZeros(text) {
    return text.replace(/\.?0+$/, "");
  }

  function formatNumber(value) {
    if (!Number.isFinite(value)) {
      return "";
    }
    const abs = Math.abs(value);
    if (abs >= 100 || abs === 0) {
      return String(Math.round(value));
    }
    if (abs >= 10) {
      return trimZeros(value.toFixed(1));
    }
    if (abs >= 1) {
      return trimZeros(value.toFixed(2));
    }
    return trimZeros(value.toFixed(3));
  }

  function jdToDate(jd) {
    if (!Number.isFinite(jd)) {
      return null;
    }
    return new Date((jd - 2440587.5) * 86400000);
  }

  function formatUtcTime(jd) {
    const date = jdToDate(jd);
    if (!date || Number.isNaN(date.getTime())) {
      return "--:--:--";
    }
    return date.toISOString().slice(11, 19);
  }

  function formatJd(jd) {
    if (!Number.isFinite(jd)) {
      return "";
    }
    return jd.toFixed(6);
  }

  function clamp(value, min, max) {
    return Math.min(max, Math.max(min, value));
  }

  function hexToRgb(hex) {
    const clean = String(hex || "").replace("#", "");
    const full = clean.length === 3
      ? clean
          .split("")
          .map(function (digit) {
            return digit + digit;
          })
          .join("")
      : clean;
    const intValue = parseInt(full, 16);
    return {
      r: (intValue >> 16) & 255,
      g: (intValue >> 8) & 255,
      b: intValue & 255,
    };
  }

  function rgbToHex(rgb) {
    function part(value) {
      const clamped = clamp(Math.round(value), 0, 255);
      return clamped.toString(16).padStart(2, "0");
    }
    return "#" + part(rgb.r) + part(rgb.g) + part(rgb.b);
  }

  function interpolateColor(a, b, t) {
    return rgbToHex({
      r: a.r + (b.r - a.r) * t,
      g: a.g + (b.g - a.g) * t,
      b: a.b + (b.b - a.b) * t,
    });
  }

  const VIRIDIS_STOPS = [
    { t: 0.0, color: "#440154" },
    { t: 0.13, color: "#482777" },
    { t: 0.25, color: "#3f4a8a" },
    { t: 0.38, color: "#31688e" },
    { t: 0.5, color: "#26828e" },
    { t: 0.63, color: "#1f9e89" },
    { t: 0.75, color: "#35b779" },
    { t: 0.88, color: "#6ece58" },
    { t: 1.0, color: "#fde725" },
  ];

  function viridisColor(norm) {
    const t = clamp(norm, 0, 1);
    for (let idx = 1; idx < VIRIDIS_STOPS.length; idx += 1) {
      const hi = VIRIDIS_STOPS[idx];
      const lo = VIRIDIS_STOPS[idx - 1];
      if (t <= hi.t + 1e-9) {
        const localT = (t - lo.t) / Math.max(hi.t - lo.t, 1e-9);
        return interpolateColor(hexToRgb(lo.color), hexToRgb(hi.color), localT);
      }
    }
    return VIRIDIS_STOPS[VIRIDIS_STOPS.length - 1].color;
  }

  function linearTicks(min, max, count) {
    if (!Number.isFinite(min) || !Number.isFinite(max)) {
      return [];
    }
    if (Math.abs(max - min) < 1e-9) {
      return [min];
    }
    const out = [];
    for (let idx = 0; idx < count; idx += 1) {
      out.push(min + ((max - min) * idx) / (count - 1));
    }
    return out;
  }

  function panelSelectionKey(sectionId, rowIdx, panelIdx) {
    return [sectionId, rowIdx, panelIdx].join(":");
  }

  function targetMaskKey(rowIdx, panelIdx) {
    return [rowIdx, panelIdx].join(":");
  }

  function normalizeJdInterval(startJd, endJd) {
    return {
      start_jd: Math.min(Number(startJd), Number(endJd)),
      end_jd: Math.max(Number(startJd), Number(endJd)),
    };
  }

  function timeFlagScopeLabel(scope) {
    const match = TIME_FLAG_SCOPES.find(function (item) {
      return item.id === scope;
    });
    return match ? match.label : String(scope || "");
  }

  function mergedSectionHeading(rowLabels) {
    const labels = Array.isArray(rowLabels) ? rowLabels.filter(Boolean) : [];
    if (!labels.length) {
      return "";
    }
    if (labels.length === 2) {
      const first = String(labels[0]).replace(/^XX?\s+/i, "").trim();
      const second = String(labels[1]).replace(/^YY?\s+/i, "").trim();
      if (first && first === second) {
        return "X & Y " + first;
      }
    }
    return labels.join(" · ");
  }

  function useMeasuredWidth(ref, fallbackWidth) {
    const [width, setWidth] = useState(fallbackWidth);

    useEffect(
      function () {
        if (!ref.current || typeof window === "undefined") {
          return;
        }
        function updateWidth() {
          if (!ref.current) {
            return;
          }
          const nextWidth = Math.max(fallbackWidth, Math.round(ref.current.getBoundingClientRect().width || fallbackWidth));
          setWidth(nextWidth);
        }
        updateWidth();
        let resizeObserver = null;
        if (typeof ResizeObserver !== "undefined") {
          resizeObserver = new ResizeObserver(updateWidth);
          resizeObserver.observe(ref.current);
        }
        window.addEventListener("resize", updateWidth);
        return function () {
          window.removeEventListener("resize", updateWidth);
          if (resizeObserver) {
            resizeObserver.disconnect();
          }
        };
      },
      [ref, fallbackWidth]
    );

    return width;
  }

  function expandTimeFlagTargets(interval, layout) {
    if (!interval || !layout) {
      return [];
    }
    const ant = clamp(Number(interval.antenna || 0), 0, Math.max(0, Number(layout.nsolant || 1) - 1));
    const band = clamp(Number(interval.band || 0), 0, Math.max(0, Number(layout.maxnbd || 1) - 1));
    const scope = String(interval.scope || "selected");
    if (scope === "selected") {
      return [{ antenna: ant, band: band }];
    }
    if (scope === "this_ant") {
      return Array.from({ length: Number(layout.maxnbd || 0) }).map(function (_item, idx) {
        return { antenna: ant, band: idx };
      });
    }
    if (scope === "this_band") {
      return Array.from({ length: Number(layout.nsolant || 0) }).map(function (_item, idx) {
        return { antenna: idx, band: band };
      });
    }
    if (scope === "higher_bands") {
      return Array.from({ length: Math.max(0, Number(layout.maxnbd || 0) - band) }).map(function (_item, idx) {
        return { antenna: ant, band: band + idx };
      });
    }
    if (scope === "all") {
      const out = [];
      for (let antIdx = 0; antIdx < Number(layout.nsolant || 0); antIdx += 1) {
        for (let bandIdx = 0; bandIdx < Number(layout.maxnbd || 0); bandIdx += 1) {
          out.push({ antenna: antIdx, band: bandIdx });
        }
      }
      return out;
    }
    return [];
  }

  function mergeTimeFlagIntervals(intervals, nextInterval) {
    const normalized = normalizeJdInterval(nextInterval.start_jd, nextInterval.end_jd);
    const mergedSeed = Object.assign({}, nextInterval, normalized);
    const tol = 1.0e-9;
    const kept = [];
    let merged = mergedSeed;
    (intervals || []).forEach(function (item) {
      const sameTarget =
        Number(item.antenna) === Number(merged.antenna)
        && Number(item.band) === Number(merged.band)
        && String(item.scope) === String(merged.scope);
      const overlaps = !(
        Number(item.end_jd) < Number(merged.start_jd) - tol
        || Number(merged.end_jd) < Number(item.start_jd) - tol
      );
      if (sameTarget && overlaps) {
        merged = Object.assign({}, merged, {
          temp_id: String(item.temp_id || merged.temp_id),
          start_jd: Math.min(Number(merged.start_jd), Number(item.start_jd)),
          end_jd: Math.max(Number(merged.end_jd), Number(item.end_jd)),
        });
      } else {
        kept.push(item);
      }
    });
    kept.push(merged);
    kept.sort(function (a, b) {
      return Number(a.start_jd) - Number(b.start_jd) || Number(a.end_jd) - Number(b.end_jd);
    });
    return kept;
  }

  function intersectKeptRanges(rangesA, rangesB, bandEdges) {
    if (!bandEdges || !bandEdges.length) {
      return [];
    }
    const maskA = bandMaskFromRanges(rangesA, bandEdges);
    const maskB = bandMaskFromRanges(rangesB, bandEdges);
    return keptRangesFromMask(
      maskA.map(function (value, idx) {
        return Boolean(value) && Boolean(maskB[idx]);
      }),
      bandEdges
    );
  }

  function defaultKeptRanges(bandEdges) {
    if (!bandEdges || !bandEdges.length) {
      return [];
    }
    return [
      {
        start_band: Number(bandEdges[0].band),
        end_band: Number(bandEdges[bandEdges.length - 1].band),
      },
    ];
  }

  function bandMaskFromRanges(ranges, bandEdges) {
    if (!bandEdges || !bandEdges.length) {
      return [];
    }
    const effectiveRanges = ranges && ranges.length ? ranges : defaultKeptRanges(bandEdges);
    return bandEdges.map(function (edge) {
      return effectiveRanges.some(function (range) {
        return Number(edge.band) >= Number(range.start_band) && Number(edge.band) <= Number(range.end_band);
      });
    });
  }

  function keptRangesFromMask(mask, bandEdges) {
    if (!bandEdges || !bandEdges.length || !mask || !mask.length) {
      return [];
    }
    const ranges = [];
    let startBand = null;
    let previousBand = null;
    bandEdges.forEach(function (edge, idx) {
      const band = Number(edge.band);
      if (mask[idx]) {
        if (startBand === null) {
          startBand = band;
        } else if (previousBand !== null && band !== previousBand + 1) {
          ranges.push({ start_band: startBand, end_band: previousBand });
          startBand = band;
        }
        previousBand = band;
      } else if (startBand !== null && previousBand !== null) {
        ranges.push({ start_band: startBand, end_band: previousBand });
        startBand = null;
        previousBand = null;
      }
    });
    if (startBand !== null && previousBand !== null) {
      ranges.push({ start_band: startBand, end_band: previousBand });
    }
    return ranges;
  }

  function excludedRangesFromKeptRanges(ranges, bandEdges) {
    if (!bandEdges || !bandEdges.length) {
      return [];
    }
    const keptMask = bandMaskFromRanges(ranges, bandEdges);
    return keptRangesFromMask(
      keptMask.map(function (value) {
        return !value;
      }),
      bandEdges
    );
  }

  function formatKeptRangesLabel(ranges, bandEdges) {
    const effectiveRanges = ranges && ranges.length ? ranges : defaultKeptRanges(bandEdges);
    if (!effectiveRanges.length) {
      return "—";
    }
    return effectiveRanges
      .map(function (range) {
        return Number(range.start_band) === Number(range.end_band)
          ? String(range.start_band)
          : String(range.start_band) + "-" + String(range.end_band);
      })
      .join(", ");
  }

  function updateKeptRanges(ranges, bandEdges, startBand, endBand, mode) {
    if (!bandEdges || !bandEdges.length) {
      return ranges || [];
    }
    const lo = Math.min(Number(startBand), Number(endBand));
    const hi = Math.max(Number(startBand), Number(endBand));
    const selectionMask = bandEdges.map(function (edge) {
      const band = Number(edge.band);
      return band >= lo && band <= hi;
    });
    const candidateMask = selectionMask.map(function (selected) {
      return !selected;
    });
    if (!candidateMask.some(Boolean)) {
      return defaultKeptRanges(bandEdges);
    }
    return keptRangesFromMask(candidateMask, bandEdges);
  }

  function excludeRangeFromKeptRanges(ranges, bandEdges, startBand, endBand) {
    if (!bandEdges || !bandEdges.length) {
      return ranges || [];
    }
    const currentMask = bandMaskFromRanges(ranges, bandEdges);
    const lo = Math.min(Number(startBand), Number(endBand));
    const hi = Math.max(Number(startBand), Number(endBand));
    const candidateMask = currentMask.map(function (keep, idx) {
      const band = Number(bandEdges[idx].band);
      return Boolean(keep) && !(band >= lo && band <= hi);
    });
    if (!candidateMask.some(Boolean)) {
      return ranges && ranges.length ? ranges : defaultKeptRanges(bandEdges);
    }
    return keptRangesFromMask(candidateMask, bandEdges);
  }

  function optimisticPanelUpdate(panel, nextRanges, bandEdges) {
    const nextPanel = Object.assign({}, panel, { kept_ranges: nextRanges });
    if (panel && typeof panel.annotation === "string" && panel.annotation.indexOf("| kept ") >= 0) {
      nextPanel.annotation = panel.annotation.replace(/\| kept .*/, "| kept " + formatKeptRangesLabel(nextRanges, bandEdges));
    }
    return nextPanel;
  }

  function mergeSectionAntennaPanels(currentSection, updateSection, antennaIndex) {
    if (!updateSection || !updateSection.panels) {
      return updateSection || currentSection;
    }
    const sparseAntennas = Array.isArray(updateSection.sparse_antennas)
      ? updateSection.sparse_antennas
          .map(function (value) {
            return Number(value);
          })
          .filter(function (value) {
            return Number.isFinite(value) && value >= 0;
          })
      : null;
    const antennaSet =
      sparseAntennas && sparseAntennas.length
        ? new Set(sparseAntennas)
        : Number.isFinite(antennaIndex) && antennaIndex >= 0
        ? new Set([antennaIndex])
        : null;
    if (!antennaSet) {
      return updateSection || currentSection;
    }
    const currentPanels = currentSection && currentSection.panels ? currentSection.panels : [];
    const mergedPanels = (updateSection.panels || []).map(function (row, rowIdx) {
      const currentRow = currentPanels[rowIdx] || [];
      return row.map(function (panel, panelIdx) {
        if (antennaSet.has(panelIdx)) {
          return panel || currentRow[panelIdx] || panel;
        }
        return currentRow[panelIdx] || panel;
      });
    });
    return Object.assign({}, currentSection || {}, updateSection, { panels: mergedPanels });
  }

  function mergeOverviewAntennaUpdates(current, updates, antennaIndex) {
    if (!updates) {
      return current;
    }
    const next = Object.assign({}, current || {});
    Object.keys(updates).forEach(function (sectionId) {
      next[sectionId] = mergeSectionAntennaPanels(next[sectionId], updates[sectionId], antennaIndex);
    });
    return next;
  }

  function legendItem(label, color, mode) {
    return html`
      <div className="series-legend-item" key=${label + "-" + color + "-" + (mode || "points")}>
        <span className=${"series-swatch " + (mode === "line" ? "series-swatch-line" : "series-swatch-point")} style=${{ "--swatch-color": color }}></span>
        <span>${label}</span>
      </div>
    `;
  }

  function seriesPairs(series) {
    const xs = (series && series.x) || [];
    const ys = (series && series.y) || [];
    const npts = Math.min(xs.length, ys.length);
    const out = [];
    for (let idx = 0; idx < npts; idx += 1) {
      const x = xs[idx];
      const y = ys[idx];
      if (Number.isFinite(x) && Number.isFinite(y)) {
        out.push([x, y]);
      }
    }
    return out;
  }

  function polylinePath(points, xMap, yMap) {
    if (!points.length) {
      return "";
    }
    return points
      .map(function (point, idx) {
        return (idx === 0 ? "M" : "L") + xMap(point[0]).toFixed(2) + " " + yMap(point[1]).toFixed(2);
      })
      .join(" ");
  }

  function wrapPhaseResidual(value) {
    return Math.atan2(Math.sin(value), Math.cos(value));
  }

  function nearestSeriesY(points, targetX) {
    if (!points.length) {
      return null;
    }
    let bestY = null;
    let bestDist = Infinity;
    for (let idx = 0; idx < points.length; idx += 1) {
      const pair = points[idx];
      const dist = Math.abs(pair[0] - targetX);
      if (dist < bestDist) {
        bestDist = dist;
        bestY = pair[1];
      }
    }
    return bestY;
  }

  function buildInbandResidualData(data, antIdx) {
    if (!data || data.message || !data.panels || antIdx < 0) {
      return null;
    }
    const panels = [];
    let xMin = Infinity;
    let xMax = -Infinity;
    let maxAbs = 0.3;
    for (let rowIdx = 0; rowIdx < 2; rowIdx += 1) {
      const row = data.panels[rowIdx] || [];
      const panel = row[antIdx];
      const rawSeries = panel
        ? (panel.series || []).filter(function (series) {
            return series.role === "raw" || series.label === "Raw phase";
          })
        : [];
      const fitSeries = panel
        ? (panel.series || []).filter(function (series) {
            return series.role === "fit" || series.label === "Fit";
          })
        : [];
      const rawPairs = rawSeries
        .map(seriesPairs)
        .reduce(function (acc, pairs) {
          return acc.concat(pairs);
        }, []);
      const fitPairs = fitSeries
        .map(seriesPairs)
        .reduce(function (acc, pairs) {
          return acc.concat(pairs);
        }, []);
      fitPairs.sort(function (a, b) {
        return a[0] - b[0];
      });
      const residualPairs = rawPairs
        .map(function (pair) {
          const fitY = nearestSeriesY(fitPairs, pair[0]);
          if (!Number.isFinite(fitY)) {
            return null;
          }
          const residual = wrapPhaseResidual(pair[1] - fitY);
          maxAbs = Math.max(maxAbs, Math.abs(residual));
          xMin = Math.min(xMin, pair[0]);
          xMax = Math.max(xMax, pair[0]);
          return [pair[0], residual];
        })
        .filter(Boolean);
      panels.push([
        {
          title: panel && panel.title ? panel.title : "Ant " + String(antIdx + 1),
          annotation: panel && panel.annotation ? panel.annotation : null,
          series: [
            {
              label: rowIdx === 0 ? "X residual" : "Y residual",
              mode: "points",
              color: rowIdx === 0 ? COLOR_X : COLOR_Y,
              x: residualPairs.map(function (pair) {
                return pair[0];
              }),
              y: residualPairs.map(function (pair) {
                return pair[1];
              }),
            },
          ],
        },
      ]);
    }
    if (!Number.isFinite(xMin) || !Number.isFinite(xMax)) {
      return {
        message: "No fitted in-band residuals are available for this antenna.",
        title: "Inband Fit Residuals",
      };
    }
    const lim = Math.min(Math.PI, Math.max(0.4, maxAbs * 1.15));
    return {
      message: null,
      type: "panel-grid",
      title: "Detrended Phase Residuals",
      x_label: data.x_label || "Frequency [GHz]",
      row_labels: ["X Residual [rad]", "Y Residual [rad]"],
      x_limits: [xMin, xMax],
      x_ticks: data.x_ticks || { values: linearTicks(xMin, xMax, 4), labels: [] },
      y_limits: [[-lim, lim], [-lim, lim]],
      panels: panels,
      legend: [],
      auto_scale_rows: false,
    };
  }

  function HeatmapPlot(props) {
    const data = props.data;
    const [hoverCell, setHoverCell] = useState(null);
    const shellRef = useRef(null);
    const shellWidth = useMeasuredWidth(shellRef, 300);
    if (!data) {
      return html`<div className="plot-placeholder">Loading heatmap...</div>`;
    }
    if (data.message) {
      return html`<div className="plot-placeholder">${data.message}</div>`;
    }
    const width = shellWidth > 0 ? shellWidth : 300;
    const height = Math.round(width * 1.05);
    const margin = { left: 48, right: 42, top: 22, bottom: 58 };
    const plotWidth = width - margin.left - margin.right;
    const plotHeight = height - margin.top - margin.bottom;
    const cellWidth = plotWidth / data.nsolant;
    const cellHeight = plotHeight / data.maxnbd;
    const selectedAnt =
      props.selectedAnt !== null && props.selectedAnt !== undefined ? props.selectedAnt : data.selected_ant;
    const selectedBand =
      props.selectedBand !== null && props.selectedBand !== undefined ? props.selectedBand : data.selected_band;
    const vmin = Number.isFinite(data.vmin) ? data.vmin : 0.0;
    const vmax = Number.isFinite(data.vmax) ? data.vmax : 2.0;
    const colorLevels = Array.isArray(data.color_levels) && data.color_levels.length ? data.color_levels : null;
    const colorBins = Array.isArray(data.color_bins) && data.color_bins.length ? data.color_bins : null;
    const pendingCellSet = new Set(
      (props.pendingCells || []).map(function (cell) {
        return String(cell.antenna) + ":" + String(cell.band);
      })
    );
    const appliedCellSet = new Set(
      ((data && data.applied_cells) || []).map(function (cell) {
        return String(cell.antenna) + ":" + String(cell.band);
      })
    );
    function colorFor(value) {
      if (colorBins) {
        for (let idx = 0; idx < colorBins.length; idx += 1) {
          const bin = colorBins[idx];
          const min = Number(bin.min);
          const max = Number(bin.max);
          if (value >= min && (value < max || (idx === colorBins.length - 1 && value <= max))) {
            return bin.color;
          }
        }
      }
      if (colorLevels) {
        const match = colorLevels.find(function (item) {
          return Number(item.value) === Number(value);
        });
        if (match) {
          return match.color;
        }
      }
      const norm = (value - vmin) / Math.max(vmax - vmin, 1e-9);
      return viridisColor(norm);
    }
    const yTicks = [];
    for (let value = 0; value <= data.maxnbd; value += 10) {
      yTicks.push(value);
    }
    const barX = margin.left + plotWidth + 16;
    const barY = margin.top;
    const barWidth = 8;
    const barHeight = plotHeight;
    const colorbarTicks = colorBins
      ? colorBins.map(function (item) {
          return 0.5 * (Number(item.min) + Number(item.max));
        })
      : colorLevels
      ? colorLevels.map(function (item) { return Number(item.value); })
      : [vmin, (vmin + vmax) / 2.0, vmax];
    const majorX = [];
    for (let idx = 0; idx <= data.nsolant; idx += 5) {
      majorX.push(idx);
    }
    if (majorX[majorX.length - 1] !== data.nsolant) {
      majorX.push(data.nsolant);
    }
    const majorY = [];
    for (let idx = 0; idx <= data.maxnbd; idx += 10) {
      majorY.push(idx);
    }
    if (majorY[majorY.length - 1] !== data.maxnbd) {
      majorY.push(data.maxnbd);
    }
    const tooltipValue =
      hoverCell && data.values && data.values[hoverCell.band] ? data.values[hoverCell.band][hoverCell.ant] : null;
    const tooltipWidth = 106;
    const tooltipHeight = 44;
    const tooltipCellX = hoverCell ? margin.left + hoverCell.ant * cellWidth : 0;
    const tooltipCellY = hoverCell ? margin.top + plotHeight - (hoverCell.band + 1) * cellHeight : 0;
    const tooltipX = clamp(tooltipCellX + cellWidth + 8, margin.left + 4, width - tooltipWidth - 6);
    const tooltipY = clamp(tooltipCellY - tooltipHeight - 6, 18, height - tooltipHeight - 18);
    function cellFromPointer(event) {
      const rect = event.currentTarget.getBoundingClientRect();
      if (!rect.width || !rect.height) {
        return null;
      }
      const localX = ((event.clientX - rect.left) / rect.width) * plotWidth;
      const localY = ((event.clientY - rect.top) / rect.height) * plotHeight;
      const antIdx = Math.max(0, Math.min(data.nsolant - 1, Math.floor(localX / cellWidth)));
      const bandFromTop = Math.max(0, Math.min(data.maxnbd - 1, Math.floor(localY / cellHeight)));
      const bandIdx = data.maxnbd - 1 - bandFromTop;
      return { ant: antIdx, band: bandIdx };
    }
    function updateHover(event) {
      const next = cellFromPointer(event);
      setHoverCell(function (current) {
        if (
          current &&
          next &&
          current.ant === next.ant &&
          current.band === next.band
        ) {
          return current;
        }
        return next;
      });
    }
    return html`
      <div className="js-plot-shell heatmap-shell" ref=${shellRef}>
        <svg
          viewBox=${"0 0 " + width + " " + height}
          className="svg-plot"
          preserveAspectRatio="xMidYMid meet"
          style=${{ aspectRatio: width + " / " + height }}
          role="img"
          aria-label=${data.title}
        >
          <defs>
            ${!colorLevels
              ? html`
                  <linearGradient id="viridis-heatmap-bar" x1="0%" y1="100%" x2="0%" y2="0%">
                    ${VIRIDIS_STOPS.map(function (stop) {
                      return html`<stop key=${"stop-" + stop.t} offset=${String(stop.t * 100) + "%"} stopColor=${stop.color} />`;
                    })}
                  </linearGradient>
                `
              : null}
          </defs>
          ${Array.from({ length: data.nsolant }).map(function (_, antIdx) {
            const x = margin.left + antIdx * cellWidth + cellWidth / 2;
            return html`
              <text
                key=${"xtick-" + antIdx}
                x=${x}
                y=${margin.top + plotHeight + 18}
                textAnchor="end"
                transform=${"rotate(-35 " + x + " " + (margin.top + plotHeight + 18) + ")"}
                className="axis-label"
              >
                ${String(antIdx + 1)}
              </text>
            `;
          })}

          ${yTicks.map(function (tick) {
            const y = margin.top + plotHeight - (tick / data.maxnbd) * plotHeight;
            return html`
              <g key=${"ytick-" + tick}>
                <line x1=${margin.left} x2=${margin.left + plotWidth} y1=${y} y2=${y} className="grid-line" />
                <line x1=${margin.left - 6} x2=${margin.left} y1=${y} y2=${y} className="axis-line" />
                <text x=${margin.left - 10} y=${y + 4} textAnchor="end" className="axis-label">${String(tick)}</text>
              </g>
            `;
          })}

          ${data.values.map(function (row, bandIdx) {
            return row.map(function (value, antIdx) {
              const x = margin.left + antIdx * cellWidth;
              const y = margin.top + plotHeight - (bandIdx + 1) * cellHeight;
              return html`
                <rect
                  key=${"cell-" + antIdx + "-" + bandIdx}
                  x=${x}
                  y=${y}
                  width=${cellWidth}
                  height=${cellHeight}
                  fill=${colorFor(value)}
                  className="heatmap-cell"
                />
              `;
            });
          })}

          ${data.values.map(function (row, bandIdx) {
            return row.map(function (_value, antIdx) {
              const key = String(antIdx) + ":" + String(bandIdx);
              if (!appliedCellSet.has(key)) {
                return null;
              }
              return html`<rect
                key=${"applied-" + key}
                x=${margin.left + antIdx * cellWidth + 0.9}
                y=${margin.top + plotHeight - (bandIdx + 1) * cellHeight + 0.9}
                width=${Math.max(cellWidth - 1.8, 0.8)}
                height=${Math.max(cellHeight - 1.8, 0.8)}
                fill="none"
                stroke="rgba(0, 113, 227, 0.58)"
                strokeWidth="1.15"
              />`;
            });
          })}

          ${data.values.map(function (row, bandIdx) {
            return row.map(function (_value, antIdx) {
              const key = String(antIdx) + ":" + String(bandIdx);
              if (!pendingCellSet.has(key)) {
                return null;
              }
              return html`<rect
                key=${"pending-" + key}
                x=${margin.left + antIdx * cellWidth + 0.5}
                y=${margin.top + plotHeight - (bandIdx + 1) * cellHeight + 0.5}
                width=${Math.max(cellWidth - 1.0, 1.0)}
                height=${Math.max(cellHeight - 1.0, 1.0)}
                fill="none"
                stroke="#1f6feb"
                strokeWidth="2.2"
              />`;
            });
          })}

          ${majorX.map(function (idx) {
            const x = margin.left + idx * cellWidth;
            return html`<line key=${"vx-major-" + idx} x1=${x} x2=${x} y1=${margin.top} y2=${margin.top + plotHeight} className="heatmap-major-line" />`;
          })}
          ${majorY.map(function (idx) {
            const y = margin.top + plotHeight - idx * cellHeight;
            return html`<line key=${"hy-major-" + idx} x1=${margin.left} x2=${margin.left + plotWidth} y1=${y} y2=${y} className="heatmap-major-line" />`;
          })}

          ${selectedAnt !== null && selectedBand !== null
            ? html`<rect
                x=${margin.left + selectedAnt * cellWidth}
                y=${margin.top + plotHeight - (selectedBand + 1) * cellHeight}
                width=${cellWidth}
                height=${cellHeight}
                fill="none"
                stroke="#ff2f00"
                strokeWidth="2.5"
              />`
            : null}
          ${hoverCell && (selectedAnt !== hoverCell.ant || selectedBand !== hoverCell.band)
            ? html`<rect
                x=${margin.left + hoverCell.ant * cellWidth}
                y=${margin.top + plotHeight - (hoverCell.band + 1) * cellHeight}
                width=${cellWidth}
                height=${cellHeight}
                className="heatmap-hover-box"
              />`
            : null}

          <rect x=${margin.left} y=${margin.top} width=${plotWidth} height=${plotHeight} className="plot-frame" />

          <text x=${margin.left + plotWidth / 2} y=${height - 8} textAnchor="middle" className="axis-label">${data.x_label}</text>
          <text
            x="18"
            y=${margin.top + plotHeight / 2}
            transform=${"rotate(-90 18 " + (margin.top + plotHeight / 2) + ")"}
            className="axis-label"
          >
            ${data.y_label}
          </text>

          ${colorBins
            ? colorBins.map(function (bin, idx) {
                const denom = Math.max(vmax - vmin, 1e-9);
                const yTop = barY + (1.0 - (Number(bin.max) - vmin) / denom) * barHeight;
                const yBottom = barY + (1.0 - (Number(bin.min) - vmin) / denom) * barHeight;
                return html`
                  <rect
                    key=${"bar-bin-" + idx}
                    x=${barX}
                    y=${Math.min(yTop, yBottom)}
                    width=${barWidth}
                    height=${Math.max(Math.abs(yBottom - yTop), 1)}
                    fill=${bin.color}
                  />
                `;
              })
            : colorLevels
            ? colorLevels
                .slice()
                .sort(function (a, b) {
                  return Number(a.value) - Number(b.value);
                })
                .map(function (level, idx, items) {
                  const bandHeight = barHeight / Math.max(items.length, 1);
                  const displayIdx = items.length - 1 - idx;
                  return html`
                    <rect
                      key=${"bar-fill-" + level.value}
                      x=${barX}
                      y=${barY + displayIdx * bandHeight}
                      width=${barWidth}
                      height=${bandHeight}
                      fill=${level.color}
                    />
                  `;
                })
            : html`<rect x=${barX} y=${barY} width=${barWidth} height=${barHeight} fill="url(#viridis-heatmap-bar)" />`}
          ${colorBins
            ? colorBins.map(function (bin, idx) {
                const y = barY + (1.0 - (0.5 * (Number(bin.min) + Number(bin.max)) - vmin) / Math.max(vmax - vmin, 1e-9)) * barHeight;
                return html`
                  <g key=${"bar-bin-label-" + idx}>
                    <line x1=${barX + barWidth} x2=${barX + barWidth + 6} y1=${y} y2=${y} className="axis-line" />
                    <text x=${barX + barWidth + 10} y=${y + 4} className="axis-label">${String(bin.label)}</text>
                  </g>
                `;
              })
            : colorLevels
            ? colorLevels
                .slice()
                .sort(function (a, b) {
                  return Number(a.value) - Number(b.value);
                })
                .map(function (level, idx, items) {
                  const bandHeight = barHeight / Math.max(items.length, 1);
                  const displayIdx = items.length - 1 - idx;
                  const y = barY + (displayIdx + 0.5) * bandHeight;
                  return html`
                    <g key=${"bar-tick-" + level.value}>
                      <line x1=${barX + barWidth} x2=${barX + barWidth + 6} y1=${y} y2=${y} className="axis-line" />
                      <text x=${barX + barWidth + 10} y=${y + 4} className="axis-label">${String(Math.round(Number(level.value)))}</text>
                    </g>
                  `;
                })
            : colorbarTicks.map(function (tick) {
                const y = barY + (1.0 - (tick - vmin) / Math.max(vmax - vmin, 1e-9)) * barHeight;
                return html`
                  <g key=${"bar-" + tick}>
                    <line x1=${barX + barWidth} x2=${barX + barWidth + 6} y1=${y} y2=${y} className="axis-line" />
                    <text x=${barX + barWidth + 10} y=${y + 4} className="axis-label">${String(Math.round(tick))}</text>
                  </g>
                `;
              })}
          <rect x=${barX} y=${barY} width=${barWidth} height=${barHeight} className="plot-frame" />
          <text x=${barX + barWidth / 2} y=${barY - 8} textAnchor="middle" className="axis-label">
            ${data.colorbar_label || ""}
          </text>
          <rect
            x=${margin.left}
            y=${margin.top}
            width=${plotWidth}
            height=${plotHeight}
            fill="rgba(0,0,0,0)"
            className="heatmap-hit-area"
            onMouseMove=${updateHover}
            onMouseLeave=${function () {
              setHoverCell(null);
            }}
            onClick=${function (event) {
              const next = cellFromPointer(event);
              if (next) {
                props.onSelect(next.ant, next.band);
              }
            }}
          />
          ${hoverCell
            ? html`<g className="heatmap-tooltip">
                <rect x=${tooltipX} y=${tooltipY} width=${tooltipWidth} height=${tooltipHeight} rx="9" className="heatmap-tooltip-box" />
                <text x=${tooltipX + 8} y=${tooltipY + 16} className="heatmap-tooltip-text">
                  ${"Ant " + String(hoverCell.ant + 1) + ", Band " + String(hoverCell.band + 1)}
                </text>
                <text x=${tooltipX + 8} y=${tooltipY + 32} className="heatmap-tooltip-subtext">
                  ${"Flag Sum: " + formatNumber(Number(tooltipValue))}
                </text>
              </g>`
            : null}
        </svg>
      </div>
    `;
  }

  function MiniPanelPlot(props) {
    const [dragState, setDragState] = useState(null);
    const [showHint, setShowHint] = useState(false);
    const clipIdRef = useRef("mini-clip-" + Math.random().toString(36).slice(2));
    const panel = props.panel || {};
    const xLimits = props.xLimits || [0, 1];
    const yLimits = props.autoYLimits || props.yLimits || [-1, 1];
    const xTicks = (props.xTicks && props.xTicks.values) || [];
    const xTickLabels = (props.xTicks && props.xTicks.labels) || [];
    const bandEdges = props.bandEdges || [];
    const width = PANEL_GRID_PANEL_WIDTH;
    const height = props.panelHeight || 128;
    const showTitle = props.showTitle !== false;
    const showXAxisLabels = props.showXAxisLabels !== false;
    const margin = {
      left: 8,
      right: 8,
      top: 8,
      bottom: showXAxisLabels ? 24 : 8,
    };
    const plotWidth = width - margin.left - margin.right;
    const plotHeight = height - margin.top - margin.bottom;
    const xMin = xLimits[0];
    const xMax = xLimits[1];
    const yMin = yLimits[0];
    const yMax = yLimits[1];
    const yTicks = linearTicks(yMin, yMax, 3);
    const activeRanges = panel.kept_ranges || [];
    const interactionMode = props.interactionMode || null;
    const interactiveTitle =
      interactionMode === "bandselect"
        ? "Drag to stage flagged range. Click " + (props.bandSelectApplyLabel || "Apply Mask") + " to apply."
            + (props.onDoubleClick ? " Double-click to clear mask." : "")
        : interactionMode === "zoom"
          ? "Drag to zoom. Shift + drag to pan. Double-click to reset."
          : props.onDoubleClick
            ? "Double-click for detailed residual view"
            : null;
    const hintText =
      interactionMode === "bandselect"
        ? "Drag to stage flagged range · Click " + (props.bandSelectApplyLabel || "Apply Mask")
            + (props.onDoubleClick ? " · Double-click to clear mask" : "")
        : interactionMode === "zoom"
          ? "Drag to zoom · Shift+drag to pan · Double-click to reset"
          : null;
    function xMap(value) {
      const denom = xMax - xMin || 1.0;
      return margin.left + ((value - xMin) / denom) * plotWidth;
    }
    function yMap(value) {
      const denom = yMax - yMin || 1.0;
      return margin.top + ((yMax - value) / denom) * plotHeight;
    }
    function xValueFromClientX(event) {
      const rect = event.currentTarget.getBoundingClientRect();
      if (!rect.width) {
        return null;
      }
      const svgX = ((event.clientX - rect.left) / rect.width) * width;
      const clampedX = clamp(svgX, margin.left, margin.left + plotWidth);
      return xMin + ((clampedX - margin.left) / Math.max(plotWidth, 1)) * (xMax - xMin || 1.0);
    }
    function yValueFromClientY(event) {
      const rect = event.currentTarget.getBoundingClientRect();
      if (!rect.height) {
        return null;
      }
      const svgY = ((event.clientY - rect.top) / rect.height) * height;
      const clampedY = clamp(svgY, margin.top, margin.top + plotHeight);
      return yMax - ((clampedY - margin.top) / Math.max(plotHeight, 1)) * (yMax - yMin || 1.0);
    }
    function bandFromXValue(xValue) {
      if (!Number.isFinite(xValue) || !bandEdges.length) {
        return null;
      }
      for (let idx = 0; idx < bandEdges.length; idx += 1) {
        const edge = bandEdges[idx];
        if (xValue >= edge.x_min && xValue <= edge.x_max) {
          return edge.band;
        }
      }
      let bestBand = null;
      let bestDist = Infinity;
      bandEdges.forEach(function (edge) {
        const dist = Math.abs(xValue - edge.x_center);
        if (dist < bestDist) {
          bestDist = dist;
          bestBand = edge.band;
        }
      });
      return bestBand;
    }
    function rangeRect(startBand, endBand) {
      if (!Number.isFinite(startBand) || !Number.isFinite(endBand) || !bandEdges.length) {
        return null;
      }
      const lo = Math.min(startBand, endBand);
      const hi = Math.max(startBand, endBand);
      const included = bandEdges.filter(function (edge) {
        return edge.band >= lo && edge.band <= hi;
      });
      if (!included.length) {
        return null;
      }
      const x0 = xMap(included[0].x_min);
      const x1 = xMap(included[included.length - 1].x_max);
      return { x: x0, width: Math.max(x1 - x0, 1.5) };
    }
    function rangeRects(ranges) {
      return (ranges || [])
        .map(function (item) {
          return rangeRect(item.start_band, item.end_band);
        })
        .filter(Boolean);
    }
    function isFullRangeSelection(ranges) {
      if (!bandEdges.length) {
        return true;
      }
      if (!ranges || ranges.length !== 1) {
        return false;
      }
      return (
        Number(ranges[0].start_band) === Number(bandEdges[0].band) &&
        Number(ranges[0].end_band) === Number(bandEdges[bandEdges.length - 1].band)
      );
    }
    function fullRangeRect() {
      if (!bandEdges.length) {
        return null;
      }
      const x0 = xMap(bandEdges[0].x_min);
      const x1 = xMap(bandEdges[bandEdges.length - 1].x_max);
      return { x: x0, width: Math.max(x1 - x0, 1.5) };
    }
    function beginPointerDrag(event) {
      if (interactionMode === "bandselect") {
        if (!props.onBandWindowSelect || !bandEdges.length) {
          return;
        }
        const xValue = xValueFromClientX(event);
        const band = bandFromXValue(xValue);
        if (!Number.isFinite(band)) {
          return;
        }
        setDragState({
          kind: "band",
          startBand: band,
          currentBand: band,
        });
      } else if (interactionMode === "zoom") {
        const xValue = xValueFromClientX(event);
        const yValue = yValueFromClientY(event);
        if (!Number.isFinite(xValue) || !Number.isFinite(yValue)) {
          return;
        }
        setDragState(
          event.shiftKey
            ? {
                kind: "pan",
                startX: xValue,
                startY: yValue,
                currentX: xValue,
                currentY: yValue,
                baseX: [xMin, xMax],
                baseY: [yMin, yMax],
              }
            : {
                kind: "zoom",
                startX: xValue,
                startY: yValue,
                currentX: xValue,
                currentY: yValue,
              }
        );
      } else {
        return;
      }
      if (event.currentTarget && event.currentTarget.setPointerCapture) {
        event.currentTarget.setPointerCapture(event.pointerId);
      }
    }
    function movePointerDrag(event) {
      if (!dragState) {
        return;
      }
      if (dragState.kind === "band") {
        const xValue = xValueFromClientX(event);
        const band = bandFromXValue(xValue);
        if (!Number.isFinite(band)) {
          return;
        }
        setDragState(function (current) {
          if (!current) {
            return current;
          }
          return Object.assign({}, current, { currentBand: band });
        });
      } else {
        const xValue = xValueFromClientX(event);
        const yValue = yValueFromClientY(event);
        if (!Number.isFinite(xValue) || !Number.isFinite(yValue)) {
          return;
        }
        setDragState(function (current) {
          if (!current) {
            return current;
          }
          return Object.assign({}, current, { currentX: xValue, currentY: yValue });
        });
      }
    }
    function endPointerDrag(event) {
      if (!dragState) {
        return;
      }
      if (event.currentTarget && event.currentTarget.releasePointerCapture) {
        try {
          event.currentTarget.releasePointerCapture(event.pointerId);
        } catch (_err) {}
      }
      if (dragState.kind === "band") {
        const xValue = xValueFromClientX(event);
        const band = bandFromXValue(xValue);
        const startBand = dragState.startBand;
        const endBand = Number.isFinite(band) ? band : dragState.currentBand;
        setDragState(null);
        if (!Number.isFinite(startBand) || !Number.isFinite(endBand) || !props.onBandWindowSelect) {
          return;
        }
        props.onBandWindowSelect(Math.min(startBand, endBand), Math.max(startBand, endBand), "flag");
        return;
      }
      if (dragState.kind === "zoom") {
        const xValue = xValueFromClientX(event);
        const yValue = yValueFromClientY(event);
        setDragState(null);
        if (!props.onViewportChange || !Number.isFinite(xValue) || !Number.isFinite(yValue)) {
          return;
        }
        const nextX = [Math.min(dragState.startX, xValue), Math.max(dragState.startX, xValue)];
        const nextY = [Math.min(dragState.startY, yValue), Math.max(dragState.startY, yValue)];
        if (Math.abs(nextX[1] - nextX[0]) < 1e-6 || Math.abs(nextY[1] - nextY[0]) < 1e-6) {
          return;
        }
        props.onViewportChange({ xLimits: nextX, yLimits: nextY });
        return;
      }
      if (dragState.kind === "pan") {
        const xValue = xValueFromClientX(event);
        const yValue = yValueFromClientY(event);
        setDragState(null);
        if (!props.onViewportChange || !Number.isFinite(xValue) || !Number.isFinite(yValue)) {
          return;
        }
        const dx = dragState.startX - xValue;
        const dy = dragState.startY - yValue;
        props.onViewportChange({
          xLimits: [dragState.baseX[0] + dx, dragState.baseX[1] + dx],
          yLimits: [dragState.baseY[0] + dy, dragState.baseY[1] + dy],
        });
      }
    }
    const hasData = (panel.series || []).some(function (series) {
      return seriesPairs(series).length > 0;
    });
    const hasExplicitSelection = props.showBandWindow && !isFullRangeSelection(activeRanges);
    const excludedWindowRects = hasExplicitSelection ? rangeRects(excludedRangesFromKeptRanges(activeRanges, bandEdges)) : [];
    const dragRect =
      dragState && dragState.kind === "band" ? rangeRect(dragState.startBand, dragState.currentBand) : null;
    const zoomRect =
      dragState && dragState.kind === "zoom"
        ? {
            x: xMap(Math.min(dragState.startX, dragState.currentX)),
            y: yMap(Math.max(dragState.startY, dragState.currentY)),
            width: Math.max(Math.abs(xMap(dragState.currentX) - xMap(dragState.startX)), 1.5),
            height: Math.max(Math.abs(yMap(dragState.currentY) - yMap(dragState.startY)), 1.5),
          }
        : null;
    return html`
      <div
        className=${"mini-plot-shell"
          + (interactionMode ? " interactive" : props.onDoubleClick ? " interactive" : "")
          + (panel.disabled ? " disabled" : "")}
        style=${{ "--mini-panel-width": String(width), "--mini-panel-height": String(height) }}
        onDoubleClick=${function () {
          if (interactionMode === "zoom" && props.onResetViewport) {
            props.onResetViewport();
            return;
          }
          if (props.onDoubleClick) {
            props.onDoubleClick();
          }
        }}
        title=${null}
        onMouseEnter=${function () {
          if (hintText) {
            setShowHint(true);
          }
        }}
        onMouseLeave=${function () {
          setShowHint(false);
        }}
      >
        ${showHint && hintText ? html`<div className="mini-plot-hint">${hintText}</div>` : null}
        <svg viewBox=${"0 0 " + width + " " + height} className="mini-plot" preserveAspectRatio="xMidYMid meet" role="img" aria-label=${panel.title || "panel"}>
          <defs>
            <clipPath id=${clipIdRef.current}>
              <rect x=${margin.left} y=${margin.top} width=${plotWidth} height=${plotHeight} />
            </clipPath>
          </defs>
          ${xTicks.map(function (tick, idx) {
            if (tick < xMin || tick > xMax) {
              return null;
            }
            const x = xMap(tick);
            const lastIdx = xTicks.length - 1;
            const isFirst = idx === 0;
            const isLast = idx === lastIdx;
            const labelX = isFirst ? x + 2 : isLast ? x - 2 : x;
            const anchor = isFirst ? "start" : isLast ? "end" : "middle";
            return html`
              <g key=${"x-" + idx}>
                <line x1=${x} x2=${x} y1=${margin.top} y2=${margin.top + plotHeight} className="grid-line" />
                ${showXAxisLabels
                  ? html`<text x=${labelX} y=${height - 6} textAnchor=${anchor} className="mini-axis-label">${xTickLabels[idx] || formatNumber(tick)}</text>`
                  : null}
              </g>
            `;
          })}
          ${yTicks.map(function (tick, idx) {
            const y = yMap(tick);
            return html`
              <g key=${"y-" + idx}>
                <line x1=${margin.left} x2=${margin.left + plotWidth} y1=${y} y2=${y} className="grid-line" />
              </g>
            `;
          })}

          <rect x=${margin.left} y=${margin.top} width=${plotWidth} height=${plotHeight} className="plot-frame" />
          ${props.showBandWindow
            ? excludedWindowRects.map(function (item, idx) {
                return html`<rect
                  key=${"excluded-range-" + idx}
                  x=${item.x}
                  y=${margin.top}
                  width=${item.width}
                  height=${plotHeight}
                  className="band-window-excluded-bg"
                />`;
              })
            : null}
          ${dragRect && props.showBandWindow
            ? html`<rect
                x=${dragRect.x}
                y=${margin.top}
                width=${dragRect.width}
                height=${plotHeight}
                className="band-window-exclude-rect"
              />`
            : null}
          ${zoomRect
            ? html`<rect
                x=${zoomRect.x}
                y=${zoomRect.y}
                width=${zoomRect.width}
                height=${zoomRect.height}
                className="band-window-drag-rect"
              />`
            : null}
          ${showTitle && panel.title
            ? html`<text
                x=${width / 2}
                y="2"
                textAnchor="middle"
                dominantBaseline="hanging"
                className="mini-panel-title"
              >${panel.title}</text>`
            : null}
          ${panel.annotation
            ? html`<text x=${margin.left + 4} y=${margin.top + 12} className="mini-annotation">${panel.annotation}</text>`
            : null}

          ${!hasData
            ? html`<text x=${width / 2} y=${margin.top + plotHeight / 2} textAnchor="middle" className="mini-empty-label">No data</text>`
            : null}

          <g clipPath=${"url(#" + clipIdRef.current + ")"}>
            ${(panel.series || []).map(function (series, idx) {
              const pairs = seriesPairs(series);
              const path = polylinePath(pairs, xMap, yMap);
              const showLine = series.mode === "line" || series.mode === "linepoints";
              const showPoints = series.mode === "points" || series.mode === "linepoints";
              return html`
                <g key=${(panel.title || "panel") + "-series-" + idx}>
                  ${showLine && path
                    ? html`<path
                        d=${path}
                        fill="none"
                        stroke=${series.color}
                        strokeWidth="1.2"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeDasharray=${series.dasharray || null}
                        opacity=${series.opacity === undefined ? 0.95 : series.opacity}
                      />`
                    : null}
                  ${showPoints
                    ? pairs.map(function (pair, pairIdx) {
                        return html`<circle
                          key=${(panel.title || "panel") + "-pt-" + idx + "-" + pairIdx}
                          cx=${xMap(pair[0])}
                          cy=${yMap(pair[1])}
                          r="1.8"
                          fill=${series.color}
                          opacity=${series.opacity === undefined ? 0.95 : series.opacity}
                        />`;
                      })
                    : null}
                </g>
              `;
            })}
            ${panel.disabled
              ? html`<rect x=${margin.left} y=${margin.top} width=${plotWidth} height=${plotHeight} fill="rgba(120, 126, 132, 0.24)" />`
              : null}
          </g>
          ${interactionMode
            ? html`<rect
                x=${margin.left}
                y=${margin.top}
                width=${plotWidth}
                height=${plotHeight}
                className="mini-band-hitbox"
                onPointerDown=${beginPointerDrag}
                onPointerMove=${movePointerDrag}
                onPointerUp=${endPointerDrag}
                onPointerLeave=${function () {}}
              />`
            : null}
        </svg>
      </div>
    `;
  }

  function PanelGridPlot(props) {
    const [rowViewports, setRowViewports] = useState({});
    const data = props.data;
    if (!data) {
      return html`<div className="plot-placeholder">Loading plot...</div>`;
    }
    if (data.message) {
      return html`<div className="plot-placeholder">${data.message}</div>`;
    }
    const rowYLimits = (data.panels || []).map(function (row, rowIdx) {
      const viewport = rowViewports[rowIdx];
      const provided = viewport && viewport.yLimits ? viewport.yLimits : (data.y_limits && data.y_limits[rowIdx]) || [-1, 1];
      if (viewport && viewport.yLimits) {
        return provided;
      }
      if (!data.auto_scale_rows) {
        return provided;
      }
      let dMin = Infinity;
      let dMax = -Infinity;
      row.forEach(function (panel) {
        (panel.series || []).forEach(function (series) {
          seriesPairs(series).forEach(function (pair) {
            if (Number.isFinite(pair[1])) {
              dMin = Math.min(dMin, pair[1]);
              dMax = Math.max(dMax, pair[1]);
            }
          });
        });
      });
      if (!Number.isFinite(dMin)) {
        return provided;
      }
      const pad = Math.max((dMax - dMin) * 0.2, 0.05);
      return [dMin - pad, dMax + pad];
    });
    function rowXLimits(rowIdx) {
      const viewport = rowViewports[rowIdx];
      return viewport && viewport.xLimits ? viewport.xLimits : data.x_limits;
    }
    return html`
      <div
        className="panel-grid-shell"
        style=${{
          "--panel-grid-panel-width": (props.panelWidth || PANEL_GRID_PANEL_WIDTH) + "px",
          "--panel-grid-panel-height": String(props.panelHeight || 92),
        }}
      >
        ${!props.hideLegend && data.legend && data.legend.length
          ? html`
              <div className="series-legend">
                ${data.legend.map(function (entry) {
                  return legendItem(entry.label, entry.color, entry.mode);
                })}
              </div>
            `
          : null}
        <div className="panel-grid-section-heading-row">
          <span className="panel-grid-section-heading">${mergedSectionHeading(data.row_labels)}</span>
          <span className="panel-grid-section-xlabel">${data.x_label}</span>
        </div>
        <div className="panel-grid-shared-body">
          <div className="panel-grid-left-rail">
            ${data.column_controls && data.column_controls.length
              ? html`<div className="panel-grid-left-rail-spacer"></div>`
              : null}
            ${(data.panels || []).map(function (_row, rowIdx) {
              const yLimits = rowYLimits[rowIdx];
              const rowYTicks = linearTicks(yLimits[0], yLimits[1], 3);
              const panelHeight = props.panelHeight || 128;
              const isBottomRow = rowIdx === (data.panels || []).length - 1;
              const axisTop = 8;
              const axisBottom = isBottomRow ? 24 : 8;
              const axisHeight = Math.max(panelHeight - axisTop - axisBottom, 1);
              const yMin = Number(yLimits[0]);
              const yMax = Number(yLimits[1]);
              const ySpan = yMax - yMin;
              return html`
                <div key=${"yaxis-" + rowIdx} className="panel-grid-yaxis-block" style=${{ height: panelHeight + "px" }}>
                  <div className="panel-grid-yaxis" aria-hidden="true">
                    ${rowYTicks.map(function (tick, tickIdx) {
                      const fraction = Math.abs(ySpan) < 1.0e-9 ? 0.5 : clamp((Number(tick) - yMin) / ySpan, 0, 1);
                      const top = axisTop + (1.0 - fraction) * axisHeight;
                      return html`<span
                        key=${"y-tick-" + rowIdx + "-" + tickIdx}
                        className="panel-grid-yaxis-label"
                        style=${{ top: top + "px" }}
                      >${formatNumber(tick)}</span>`;
                    })}
                  </div>
                </div>
              `;
            })}
          </div>
          <div className="panel-grid-scroll">
            ${data.column_controls && data.column_controls.length
              ? html`
                  <div className="panel-grid-column-controls">
                    <div className="panel-grid-panels">
                      ${data.column_controls.map(function (control) {
                        return html`
                          <label
                            key=${"col-control-" + control.antenna}
                            className=${"panel-grid-column-control"
                              + (control.flagged ? " flagged" : "")
                              + (control.auto_flagged ? " auto-flagged" : "")}
                          >
                            <input
                              type="checkbox"
                              checked=${!!control.checked}
                              disabled=${!!props.busy}
                              onChange=${props.onColumnToggle
                                ? function (event) {
                                    props.onColumnToggle(Number(control.antenna), !event.target.checked);
                                  }
                                : null}
                            />
                            <span>${control.label}</span>
                          </label>
                        `;
                      })}
                    </div>
                  </div>
                `
              : null}
            <div className="panel-grid-rows">
              ${(data.panels || []).map(function (row, rowIdx) {
                const yLimits = rowYLimits[rowIdx];
                return html`
                  <section key=${"row-" + rowIdx} className="panel-grid-row">
                    <div className="panel-grid-panels">
                      ${row.map(function (panel, panelIdx) {
                        const displayPanel = props.panelOverride ? props.panelOverride(rowIdx, panelIdx, panel) : panel;
                        return html`<${MiniPanelPlot}
                          key=${"panel-" + rowIdx + "-" + panelIdx}
                          panel=${displayPanel}
                          showTitle=${rowIdx === 0}
                          xLimits=${rowXLimits(rowIdx)}
                          xTicks=${data.x_ticks}
                          yLimits=${data.y_limits[rowIdx]}
                          autoYLimits=${yLimits}
                          panelHeight=${props.panelHeight}
                          bandEdges=${data.band_edges}
                          showBandWindow=${!!props.onBandWindowSelect}
                          showXAxisLabels=${rowIdx === (data.panels || []).length - 1}
                          interactionMode=${props.interactionMode === "zoom" ? "zoom" : props.onBandWindowSelect ? "bandselect" : null}
                          bandSelectApplyLabel=${props.bandSelectApplyLabel}
                          onBandWindowSelect=${props.onBandWindowSelect
                            ? function (startBand, endBand, mode) {
                                props.onBandWindowSelect(rowIdx, panelIdx, startBand, endBand, mode, displayPanel);
                              }
                            : null}
                          onViewportChange=${props.interactionMode === "zoom"
                            ? function (nextViewport) {
                                setRowViewports(function (current) {
                                  return Object.assign({}, current, { [rowIdx]: nextViewport });
                                });
                              }
                            : null}
                          onResetViewport=${props.interactionMode === "zoom"
                            ? function () {
                                setRowViewports(function (current) {
                                  const next = Object.assign({}, current);
                                  delete next[rowIdx];
                                  return next;
                                });
                              }
                            : null}
                          onDoubleClick=${props.onPanelDoubleClick
                            ? function () {
                                props.onPanelDoubleClick(rowIdx, panelIdx, displayPanel);
                              }
                            : null}
                        />`;
                      })}
                    </div>
                  </section>
                `;
              })}
            </div>
          </div>
        </div>
      </div>
    `;
  }

  function ResidualInspectorModal(props) {
    if (!props.data) {
      return null;
    }
    return html`
      <div
        className="residual-modal-backdrop"
        onClick=${props.onClose}
        role="dialog"
        aria-modal="true"
        aria-label="Inband fit residuals"
      >
        <div className="residual-modal-card panel" onClick=${function (event) { event.stopPropagation(); }}>
          <div className="residual-modal-header">
            <div>
              <h2>${props.title}</h2>
              <div className="plot-card-caption-row">Double-clicked from Inband Fit. Residual = wrapped(raw phase - fitted phase).</div>
            </div>
            <button type="button" className="residual-modal-close" onClick=${props.onClose}>Close</button>
          </div>
          <div className="residual-modal-body">
            <${PanelGridPlot}
              data=${props.data}
              hideLegend=${true}
              panelWidth=${760}
              panelHeight=${170}
            />
          </div>
        </div>
      </div>
    `;
  }

  function PlotCard(props) {
    return html`
      <section className="plot-card">
        <div className="plot-card-header">
          <div className="plot-card-title-row">
            <h2>${props.title}</h2>
            ${(props.legend || []).map(function (entry) {
              return legendItem(entry.label, entry.color, entry.mode);
            })}
            ${props.inlineControls ? html`<div className="plot-card-inline-controls">${props.inlineControls}</div>` : null}
            ${props.meta ? html`<span className="plot-card-meta">${props.meta}</span>` : null}
          </div>
          ${props.controls ? html`<div className="plot-card-controls-row">${props.controls}</div>` : null}
          ${props.caption ? html`<div className="plot-card-caption-row">${props.caption}</div>` : null}
        </div>
        <div className="plot-card-body">${props.children}</div>
      </section>
    `;
  }

  function InbandWindowControls(props) {
    return html`
      <div className="inband-window-controls">
        <div className="inband-scope-group">
          <span className="tiny">Antennas</span>
          <div className="scope-pills" role="group" aria-label="Inband antenna scope">
            ${INBAND_SCOPE_OPTIONS.map(function (item) {
              return html`
                <button
                  type="button"
                  key=${"ant-scope-" + item.id}
                  className=${"scope-pill" + (props.antennaScope === item.id ? " active" : "")}
                  disabled=${props.busy}
                  onClick=${function () {
                    props.onAntennaScopeChange(item.id);
                  }}
                >
                  ${item.label}
                </button>
              `;
            })}
          </div>
        </div>
        <div className="inband-scope-group">
          <span className="tiny">Polarizations</span>
          <div className="scope-pills" role="group" aria-label="Inband polarization scope">
            ${INBAND_SCOPE_OPTIONS.map(function (item) {
              return html`
                <button
                  type="button"
                  key=${"pol-scope-" + item.id}
                  className=${"scope-pill" + (props.polScope === item.id ? " active" : "")}
                  disabled=${props.busy}
                  onClick=${function () {
                    props.onPolScopeChange(item.id);
                  }}
                >
                  ${item.label}
                </button>
              `;
            })}
          </div>
        </div>
        <button
          type="button"
          className="btn-outline-blue"
          disabled=${props.busy || !props.hasPending}
          onClick=${function () {
            props.onApply();
          }}
        >
          Apply Mask
        </button>
      </div>
    `;
  }

  function TimeHistoryPlot(props) {
    const data = props.data;
    const wrapperRef = useRef(null);
    const svgRef = useRef(null);
    const [hoverOffset, setHoverOffset] = useState(null);
    const [hoverJd, setHoverJd] = useState(null);
    const [pendingAnchorJd, setPendingAnchorJd] = useState(null);
    const [dragState, setDragState] = useState(null);
    const [hoveredGroupId, setHoveredGroupId] = useState(null);

    useEffect(
      function () {
        setHoverOffset(null);
        setHoverJd(null);
        setPendingAnchorJd(null);
        setDragState(null);
        setHoveredGroupId(null);
      },
      [data && data.title]
    );

    if (!data) {
      return html`<div className="plot-placeholder">Loading legacy-equivalent time history...</div>`;
    }
    if (data.message) {
      return html`<div className="plot-placeholder">${data.message}</div>`;
    }
    const width = 940;
    const height = 560;
    const outer = { left: 64, right: 28, top: 58, bottom: 46 };
    const gap = 52;
    const panelWidth = (width - outer.left - outer.right - gap) / 2;
    const panelHeight = height - outer.top - outer.bottom;
    const ampX0 = outer.left;
    const phaseX0 = outer.left + panelWidth + gap;
    const y0 = outer.top;
    const xOffsets = data.offset_min || [];
    const xMin = xOffsets.length ? xOffsets[0] : 0.0;
    const xMax = xOffsets.length ? xOffsets[xOffsets.length - 1] : xMin + 1.0;
    const startJd = Number.isFinite(data.start_jd) ? data.start_jd : 0.0;
    const ampMin = Math.max((data.amp_ylim && data.amp_ylim[0]) || 1e-3, 1e-6);
    const ampMax = Math.max((data.amp_ylim && data.amp_ylim[1]) || 1.0, ampMin * 10.0);
    const phaseMin = (data.phase_ylim && data.phase_ylim[0]) || -4.0;
    const phaseMax = (data.phase_ylim && data.phase_ylim[1]) || 4.0;
    const intervalGroups = data.interval_groups || [];
    const pendingIntervals = props.pendingIntervals || [];
    const allIntervalGroups = intervalGroups.concat(pendingIntervals);

    function focusShell() {
      if (wrapperRef.current && typeof wrapperRef.current.focus === "function") {
        wrapperRef.current.focus({ preventScroll: true });
      }
    }

    function xMap(value, panelStart) {
      const denom = xMax - xMin || 1.0;
      return panelStart + ((value - xMin) / denom) * panelWidth;
    }
    function offsetToJd(offset) {
      return startJd + offset / 1440.0;
    }
    function svgXFromEvent(event) {
      if (!svgRef.current) {
        return null;
      }
      const rect = svgRef.current.getBoundingClientRect();
      if (!rect.width) {
        return null;
      }
      return ((event.clientX - rect.left) / rect.width) * width;
    }
    function offsetFromSvgX(svgX, panelStart) {
      const clampedX = clamp(svgX, panelStart, panelStart + panelWidth);
      const denom = panelWidth || 1.0;
      return xMin + ((clampedX - panelStart) / denom) * (xMax - xMin || 1.0);
    }
    function intervalGroupAtOffset(offset) {
      const active = allIntervalGroups.filter(function (group) {
        const left = Math.min(group.start_offset_min, group.end_offset_min);
        const right = Math.max(group.start_offset_min, group.end_offset_min);
        return offset >= left && offset <= right;
      });
      if (!active.length) {
        return null;
      }
      active.sort(function (a, b) {
        const spanA = Math.abs(a.end_offset_min - a.start_offset_min);
        const spanB = Math.abs(b.end_offset_min - b.start_offset_min);
        return spanA - spanB;
      });
      return active[0].group_id;
    }
    function updateHover(event, panelStart) {
      const svgX = svgXFromEvent(event);
      if (!Number.isFinite(svgX)) {
        return null;
      }
      const offset = offsetFromSvgX(svgX, panelStart);
      setHoverOffset(offset);
      setHoverJd(offsetToJd(offset));
      setHoveredGroupId(intervalGroupAtOffset(offset));
      return offset;
    }
    function ampMap(value) {
      const lv = Math.log10(Math.max(value, ampMin));
      const lmin = Math.log10(ampMin);
      const lmax = Math.log10(ampMax);
      const denom = lmax - lmin || 1.0;
      return y0 + ((lmax - lv) / denom) * panelHeight;
    }
    function phaseMap(value) {
      const denom = phaseMax - phaseMin || 1.0;
      return y0 + ((phaseMax - value) / denom) * panelHeight;
    }
    function ampTicks() {
      const out = [];
      const start = Math.ceil(Math.log10(ampMin));
      const end = Math.floor(Math.log10(ampMax));
      for (let power = start; power <= end; power += 1) {
        out.push(Math.pow(10, power));
      }
      if (!out.length) {
        out.push(ampMin, ampMax);
      }
      return out;
    }
    function phaseTicks() {
      const out = [];
      for (let value = phaseMin; value <= phaseMax + 1e-6; value += 2) {
        out.push(value);
      }
      return out;
    }
    function pointElements(series, key, xStart, yMapFn) {
      return (series[key] || []).map(function (value, idx) {
        const x = xOffsets[idx];
        if (!Number.isFinite(x) || !Number.isFinite(value)) {
          return null;
        }
        if (key === "amp" && value <= 0) {
          return null;
        }
        return html`<circle
          key=${series.label + "-" + key + "-" + idx}
          cx=${xMap(x, xStart)}
          cy=${yMapFn(value)}
          r="3.2"
          fill=${series.color}
          opacity="0.95"
        />`;
      });
    }
    function ampTickLines(panelStart) {
      return ampTicks().map(function (value) {
        const y = ampMap(value);
        const power = Math.round(Math.log10(value));
        return html`
          <g key=${panelStart + "-amp-tick-" + value}>
            <line x1=${panelStart} x2=${panelStart + panelWidth} y1=${y} y2=${y} className="grid-line" />
            <line x1=${panelStart - 6} x2=${panelStart} y1=${y} y2=${y} className="axis-line" />
            <text x=${panelStart - 10} y=${y + 4} textAnchor="end" className="axis-label">
              10<tspan dy="-4" fontSize="75%">${String(power)}</tspan>
            </text>
          </g>
        `;
      });
    }
    function tickLines(panelStart, yTicks, yMapFn, formatter) {
      return yTicks.map(function (value) {
        const y = yMapFn(value);
        return html`
          <g key=${panelStart + "-tick-" + value}>
            <line x1=${panelStart} x2=${panelStart + panelWidth} y1=${y} y2=${y} className="grid-line" />
            <line x1=${panelStart - 6} x2=${panelStart} y1=${y} y2=${y} className="axis-line" />
            <text x=${panelStart - 10} y=${y + 4} textAnchor="end" className="axis-label">${formatter(value)}</text>
          </g>
        `;
      });
    }
    function xTicks(panelStart) {
      const ticks = data.tick_offsets || [];
      const labels = data.tick_labels || [];
      return ticks.map(function (value, idx) {
        const x = xMap(value, panelStart);
        return html`
          <g key=${panelStart + "-xtick-" + idx}>
            <line x1=${x} x2=${x} y1=${y0} y2=${y0 + panelHeight} className="grid-line" />
            <line x1=${x} x2=${x} y1=${y0 + panelHeight} y2=${y0 + panelHeight + 6} className="axis-line" />
            <text x=${x} y=${y0 + panelHeight + 22} textAnchor="middle" className="axis-label">${labels[idx]}</text>
          </g>
        `;
      });
    }
    function intervalBands(panelStart) {
      return allIntervalGroups.map(function (group) {
        const leftOffset = Math.min(group.start_offset_min, group.end_offset_min);
        const rightOffset = Math.max(group.start_offset_min, group.end_offset_min);
        const left = xMap(leftOffset, panelStart);
        const right = xMap(rightOffset, panelStart);
        const hovered = hoveredGroupId === group.group_id;
        return html`
          <g key=${panelStart + "-interval-" + group.group_id} className=${hovered ? "interval-group hovered" : "interval-group"}>
            <rect x=${left} y=${y0} width=${Math.max(right - left, 1.5)} height=${panelHeight} className="interval-band" />
            <line x1=${left} x2=${left} y1=${y0} y2=${y0 + panelHeight} className="interval-boundary interval-start" />
            <line x1=${right} x2=${right} y1=${y0} y2=${y0 + panelHeight} className="interval-boundary interval-end" />
          </g>
        `;
      });
    }
    function deleteIntervalGroup(groupId) {
      if (String(groupId).indexOf("staged:") === 0) {
        props.onDeletePendingInterval(groupId);
      } else {
        props.onDeleteInterval(groupId);
      }
    }
    function intervalDeleteButtons(panelStart) {
      const buttonSize = 15;
      return allIntervalGroups.map(function (group) {
        const leftOffset = Math.min(group.start_offset_min, group.end_offset_min);
        const rightOffset = Math.max(group.start_offset_min, group.end_offset_min);
        const left = xMap(leftOffset, panelStart);
        const right = xMap(rightOffset, panelStart);
        const x = clamp(right - buttonSize - 2, left + 1, panelStart + panelWidth - buttonSize - 1);
        const y = y0 + 2;
        const hovered = hoveredGroupId === group.group_id;
        return html`
          <g
            key=${panelStart + "-interval-delete-" + group.group_id}
            className=${hovered ? "interval-delete hovered" : "interval-delete"}
            onMouseEnter=${function () {
              setHoveredGroupId(group.group_id);
            }}
            onMouseLeave=${function () {
              setHoveredGroupId(function (current) {
                return current === group.group_id ? null : current;
              });
            }}
            onPointerDown=${function (event) {
              event.stopPropagation();
            }}
            onClick=${function (event) {
              event.stopPropagation();
              deleteIntervalGroup(group.group_id);
            }}
          >
            <rect x=${x} y=${y} width=${buttonSize} height=${buttonSize} rx="3" ry="3" className="interval-delete-bg" />
            <text x=${x + buttonSize / 2} y=${y + buttonSize / 2 + 0.5} textAnchor="middle" dominantBaseline="middle" className="interval-delete-label">
              ×
            </text>
          </g>
        `;
      });
    }
    function crosshair(panelStart) {
      if (!Number.isFinite(hoverOffset)) {
        return null;
      }
      const x = xMap(hoverOffset, panelStart);
      return html`<line x1=${x} x2=${x} y1=${y0} y2=${y0 + panelHeight} className="crosshair-line" />`;
    }
    function pendingAnchorLine(panelStart) {
      if (!Number.isFinite(pendingAnchorJd)) {
        return null;
      }
      const offset = (pendingAnchorJd - startJd) * 1440.0;
      const x = xMap(offset, panelStart);
      return html`<line x1=${x} x2=${x} y1=${y0} y2=${y0 + panelHeight} className="anchor-line" />`;
    }
    function dragBand(panelStart) {
      if (!dragState) {
        return null;
      }
      const left = xMap(Math.min(dragState.startOffset, dragState.currentOffset), panelStart);
      const right = xMap(Math.max(dragState.startOffset, dragState.currentOffset), panelStart);
      return html`<rect x=${left} y=${y0} width=${Math.max(right - left, 1.5)} height=${panelHeight} className="drag-band" />`;
    }
    function pointerDown(event, panelStart) {
      if (props.busy) {
        return;
      }
      focusShell();
      const offset = updateHover(event, panelStart);
      if (!Number.isFinite(offset)) {
        return;
      }
      setPendingAnchorJd(null);
      setDragState({
        startOffset: offset,
        currentOffset: offset,
        startClientX: event.clientX,
        currentClientX: event.clientX,
      });
      if (event.currentTarget && event.currentTarget.setPointerCapture) {
        event.currentTarget.setPointerCapture(event.pointerId);
      }
      if (props.onStatus) {
        props.onStatus("Drag to stage flagged range. Click Apply Mask to apply.");
      }
    }
    function pointerMove(event, panelStart) {
      const offset = updateHover(event, panelStart);
      if (!Number.isFinite(offset)) {
        return;
      }
      setDragState(function (current) {
        if (!current) {
          return current;
        }
        return Object.assign({}, current, {
          currentOffset: offset,
          currentClientX: event.clientX,
        });
      });
    }
    function pointerUp(event, panelStart) {
      const offsetRaw = updateHover(event, panelStart);
      const offset = Number.isFinite(offsetRaw) ? offsetRaw : dragState ? dragState.currentOffset : null;
      if (event.currentTarget && event.currentTarget.releasePointerCapture) {
        try {
          event.currentTarget.releasePointerCapture(event.pointerId);
        } catch (_err) {}
      }
      if (!dragState || !Number.isFinite(offset)) {
        setDragState(null);
        return;
      }
      const start = offsetToJd(dragState.startOffset);
      const end = offsetToJd(offset);
      const dragWidthPx = Math.abs((Number.isFinite(event.clientX) ? event.clientX : dragState.currentClientX) - dragState.startClientX);
      setDragState(null);
      if (dragWidthPx < TIME_FLAG_MIN_DRAG_PX || Math.abs(end - start) <= 1.0e-9) {
        if (props.onStatus) {
          props.onStatus("Ignored click without drag.");
        }
        return;
      }
      props.onStageInterval(start, end);
    }
    function pointerLeave() {
      if (!dragState) {
        setHoverOffset(null);
        setHoverJd(null);
        setHoveredGroupId(null);
      }
    }
    function deleteHoveredInterval() {
      if (!hoveredGroupId) {
        if (props.onStatus) {
          props.onStatus("X ignored. Hover an interval first.");
        }
        return;
      }
      deleteIntervalGroup(hoveredGroupId);
    }
    function handleKeyDown(event) {
      const target = event.target;
      if (target && target.tagName && /^(INPUT|TEXTAREA|SELECT|BUTTON)$/.test(target.tagName)) {
        return;
      }
      const key = String(event.key || "").toUpperCase();
      if (key === "A") {
        event.preventDefault();
        if (!Number.isFinite(hoverJd)) {
          if (props.onStatus) {
            props.onStatus("A ignored. Hover inside Time History first.");
          }
          return;
        }
        setPendingAnchorJd(hoverJd);
        if (props.onStatus) {
          props.onStatus("A anchor set at " + formatUtcTime(hoverJd) + " UT.");
        }
      } else if (key === "B") {
        event.preventDefault();
        if (!Number.isFinite(pendingAnchorJd) || !Number.isFinite(hoverJd)) {
          if (props.onStatus) {
            props.onStatus("B ignored. Set A first, then hover a second time.");
          }
          return;
        }
        props.onStageInterval(pendingAnchorJd, hoverJd);
        setPendingAnchorJd(null);
      } else if (key === "X") {
        event.preventDefault();
        deleteHoveredInterval();
      } else if (key === "ESCAPE") {
        event.preventDefault();
        setPendingAnchorJd(null);
        if (props.onStatus) {
          props.onStatus("Cleared pending A anchor.");
        }
      }
    }
    const hoverReadout = Number.isFinite(hoverJd)
      ? "UT " + formatUtcTime(hoverJd) + " | JD " + formatJd(hoverJd)
      : "Hover inside the plot";
    return html`
      <div className="time-history-shell" ref=${wrapperRef} tabIndex="0" onKeyDown=${handleKeyDown}>
        <div className="time-history-toolbar">
          <div className="scope-pills" role="group" aria-label="Time-flag scope">
            ${TIME_FLAG_SCOPES.map(function (item) {
              return html`
                <button
                  type="button"
                  key=${item.id}
                  className=${"scope-pill" + (props.scope === item.id ? " active" : "")}
                  disabled=${props.busy}
                  onClick=${function () {
                    props.onScopeChange(item.id);
                  }}
                >
                  ${item.label}
                </button>
              `;
            })}
          </div>
          <div className="time-history-readout">
            <span>${hoverReadout}</span>
            ${Number.isFinite(pendingAnchorJd)
              ? html`<span>${"A = " + formatUtcTime(pendingAnchorJd) + " UT"}</span>`
              : null}
          </div>
        </div>
        <div className="series-legend">
          ${(data.series || []).map(function (series) {
            return legendItem(series.label, series.color, "points");
          })}
        </div>
        <svg
          ref=${svgRef}
          viewBox=${"0 0 " + width + " " + height}
          className="svg-plot"
          role="img"
          aria-label=${"Time history " + data.title}
          onMouseEnter=${focusShell}
        >
          ${ampTickLines(ampX0)}
          ${tickLines(phaseX0, phaseTicks(), phaseMap, function (value) {
            return String(value);
          })}
          ${xTicks(ampX0)}
          ${xTicks(phaseX0)}
          ${intervalBands(ampX0)}
          ${intervalBands(phaseX0)}
          ${dragBand(ampX0)}
          ${dragBand(phaseX0)}
          ${crosshair(ampX0)}
          ${crosshair(phaseX0)}
          ${pendingAnchorLine(ampX0)}
          ${pendingAnchorLine(phaseX0)}

          <rect x=${ampX0} y=${y0} width=${panelWidth} height=${panelHeight} className="plot-frame" />
          <rect x=${phaseX0} y=${y0} width=${panelWidth} height=${panelHeight} className="plot-frame" />

          <text x=${ampX0 + panelWidth / 2} y=${y0 - 12} textAnchor="middle" className="panel-title">Amplitude</text>
          <text x=${phaseX0 + panelWidth / 2} y=${y0 - 12} textAnchor="middle" className="panel-title">Phase</text>
          <text x=${ampX0 + panelWidth / 2} y=${height - 8} textAnchor="middle" className="axis-label">Time [UT]</text>
          <text x=${phaseX0 + panelWidth / 2} y=${height - 8} textAnchor="middle" className="axis-label">Time [UT]</text>
          <text x="18" y=${y0 + panelHeight / 2} transform=${"rotate(-90 18 " + (y0 + panelHeight / 2) + ")"} className="axis-label">Amplitude [arb units]</text>
          <text
            x=${phaseX0 - 40}
            y=${y0 + panelHeight / 2}
            transform=${"rotate(-90 " + (phaseX0 - 40) + " " + (y0 + panelHeight / 2) + ")"}
            className="axis-label"
          >
            Phase [rad]
          </text>

          ${(data.series || []).map(function (series) {
            return pointElements(series, "amp", ampX0, ampMap);
          })}
          ${(data.series || []).map(function (series) {
            return pointElements(series, "phase", phaseX0, phaseMap);
          })}
          <rect
            x=${ampX0}
            y=${y0}
            width=${panelWidth}
            height=${panelHeight}
            className="time-history-hitbox"
            onPointerDown=${function (event) {
              pointerDown(event, ampX0);
            }}
            onPointerMove=${function (event) {
              pointerMove(event, ampX0);
            }}
            onPointerUp=${function (event) {
              pointerUp(event, ampX0);
            }}
            onPointerLeave=${pointerLeave}
          />
          <rect
            x=${phaseX0}
            y=${y0}
            width=${panelWidth}
            height=${panelHeight}
            className="time-history-hitbox"
            onPointerDown=${function (event) {
              pointerDown(event, phaseX0);
            }}
            onPointerMove=${function (event) {
              pointerMove(event, phaseX0);
            }}
            onPointerUp=${function (event) {
              pointerUp(event, phaseX0);
            }}
            onPointerLeave=${pointerLeave}
          />
          ${intervalDeleteButtons(ampX0)}
        </svg>
      </div>
    `;
  }

  function CompactTimeHistoryPlot(props) {
    const data = props.data;
    const wrapperRef = useRef(null);
    const ampSvgRef = useRef(null);
    const phaseSvgRef = useRef(null);
    const shellWidth = useMeasuredWidth(wrapperRef, 300);
    const [hoverOffset, setHoverOffset] = useState(null);
    const [hoverJd, setHoverJd] = useState(null);
    const [pendingAnchorJd, setPendingAnchorJd] = useState(null);
    const [dragState, setDragState] = useState(null);
    const [hoveredGroupId, setHoveredGroupId] = useState(null);

    useEffect(
      function () {
        setHoverOffset(null);
        setHoverJd(null);
        setPendingAnchorJd(null);
        setDragState(null);
        setHoveredGroupId(null);
      },
      [data && data.title]
    );

    if (!data) {
      return html`<div className="plot-placeholder">Loading legacy-equivalent time history...</div>`;
    }
    if (data.message) {
      return html`<div className="plot-placeholder">${data.message}</div>`;
    }

    const width = shellWidth > 0 ? shellWidth : 300;
    const ampHeight = Math.round(width * 0.42);
    const phaseHeight = Math.round(width * 0.50);
    const ampOuter = { left: 48, right: 8, top: 8, bottom: 10 };
    const phaseOuter = { left: 48, right: 8, top: 6, bottom: 30 };
    const plotWidth = width - ampOuter.left - ampOuter.right;
    const ampPlotHeight = ampHeight - ampOuter.top - ampOuter.bottom;
    const phasePlotHeight = phaseHeight - phaseOuter.top - phaseOuter.bottom;
    const xOffsets = data.offset_min || [];
    const xMin = xOffsets.length ? xOffsets[0] : 0.0;
    const xMax = xOffsets.length ? xOffsets[xOffsets.length - 1] : xMin + 1.0;
    const startJd = Number.isFinite(data.start_jd) ? data.start_jd : 0.0;
    const ampMin = Math.max((data.amp_ylim && data.amp_ylim[0]) || 1e-3, 1e-6);
    const ampMax = Math.max((data.amp_ylim && data.amp_ylim[1]) || 1.0, ampMin * 10.0);
    const phaseMin = (data.phase_ylim && data.phase_ylim[0]) || -4.0;
    const phaseMax = (data.phase_ylim && data.phase_ylim[1]) || 4.0;
    const intervalGroups = data.interval_groups || [];
    const pendingIntervals = props.pendingIntervals || [];
    const allIntervalGroups = intervalGroups.concat(pendingIntervals);

    function focusShell() {
      if (wrapperRef.current && typeof wrapperRef.current.focus === "function") {
        wrapperRef.current.focus({ preventScroll: true });
      }
    }

    function xMap(value) {
      const denom = xMax - xMin || 1.0;
      return ampOuter.left + ((value - xMin) / denom) * plotWidth;
    }
    function offsetToJd(offset) {
      return startJd + offset / 1440.0;
    }
    function svgXFromEvent(event, svgRef, svgWidth) {
      if (!svgRef.current) {
        return null;
      }
      const rect = svgRef.current.getBoundingClientRect();
      if (!rect.width) {
        return null;
      }
      return ((event.clientX - rect.left) / rect.width) * svgWidth;
    }
    function offsetFromSvgX(svgX) {
      const clampedX = clamp(svgX, ampOuter.left, ampOuter.left + plotWidth);
      const denom = plotWidth || 1.0;
      return xMin + ((clampedX - ampOuter.left) / denom) * (xMax - xMin || 1.0);
    }
    function intervalGroupAtOffset(offset) {
      const active = allIntervalGroups.filter(function (group) {
        const left = Math.min(group.start_offset_min, group.end_offset_min);
        const right = Math.max(group.start_offset_min, group.end_offset_min);
        return offset >= left && offset <= right;
      });
      if (!active.length) {
        return null;
      }
      active.sort(function (a, b) {
        const spanA = Math.abs(a.end_offset_min - a.start_offset_min);
        const spanB = Math.abs(b.end_offset_min - b.start_offset_min);
        return spanA - spanB;
      });
      return active[0].group_id;
    }
    function updateHover(event, svgRef, svgWidth) {
      const svgX = svgXFromEvent(event, svgRef, svgWidth);
      if (!Number.isFinite(svgX)) {
        return null;
      }
      const offset = offsetFromSvgX(svgX);
      setHoverOffset(offset);
      setHoverJd(offsetToJd(offset));
      setHoveredGroupId(intervalGroupAtOffset(offset));
      return offset;
    }
    function ampMap(value) {
      const lv = Math.log10(Math.max(value, ampMin));
      const lmin = Math.log10(ampMin);
      const lmax = Math.log10(ampMax);
      const denom = lmax - lmin || 1.0;
      return ampOuter.top + ((lmax - lv) / denom) * ampPlotHeight;
    }
    function phaseMap(value) {
      const denom = phaseMax - phaseMin || 1.0;
      return phaseOuter.top + ((phaseMax - value) / denom) * phasePlotHeight;
    }
    function ampTicks() {
      const out = [];
      const start = Math.ceil(Math.log10(ampMin));
      const end = Math.floor(Math.log10(ampMax));
      for (let power = start; power <= end; power += 1) {
        out.push(Math.pow(10, power));
      }
      if (!out.length) {
        out.push(ampMin, ampMax);
      }
      return out;
    }
    function phaseTicks() {
      return linearTicks(phaseMin, phaseMax, 3);
    }
    function pointElements(series, key, yMapFn) {
      return (series[key] || []).map(function (value, idx) {
        const x = xOffsets[idx];
        if (!Number.isFinite(x) || !Number.isFinite(value)) {
          return null;
        }
        if (key === "amp" && value <= 0) {
          return null;
        }
        return html`<circle
          key=${series.label + "-" + key + "-" + idx}
          cx=${xMap(x)}
          cy=${yMapFn(value)}
          r="2.3"
          fill=${series.color}
          opacity="0.95"
        />`;
      });
    }
    function ampTickLines() {
      return ampTicks().map(function (value) {
        const y = ampMap(value);
        const power = Math.round(Math.log10(value));
        return html`
          <g key=${"amp-tick-" + value}>
            <line x1=${ampOuter.left} x2=${ampOuter.left + plotWidth} y1=${y} y2=${y} className="grid-line" />
            <line x1=${ampOuter.left - 5} x2=${ampOuter.left} y1=${y} y2=${y} className="axis-line" />
            <text x=${ampOuter.left - 8} y=${y + 4} textAnchor="end" className="axis-label">
              10<tspan dy="-4" fontSize="75%">${String(power)}</tspan>
            </text>
          </g>
        `;
      });
    }
    function phaseTickLines() {
      return phaseTicks().map(function (value) {
        const y = phaseMap(value);
        return html`
          <g key=${"phase-tick-" + value}>
            <line x1=${phaseOuter.left} x2=${phaseOuter.left + plotWidth} y1=${y} y2=${y} className="grid-line" />
            <line x1=${phaseOuter.left - 5} x2=${phaseOuter.left} y1=${y} y2=${y} className="axis-line" />
            <text x=${phaseOuter.left - 8} y=${y + 4} textAnchor="end" className="axis-label">${formatNumber(value)}</text>
          </g>
        `;
      });
    }
    function phaseXTicks() {
      const ticks = data.tick_offsets || [];
      const labels = data.tick_labels || [];
      return ticks.map(function (value, idx) {
        const x = xMap(value);
        return html`
          <g key=${"phase-xtick-" + idx}>
            <line x1=${x} x2=${x} y1=${phaseOuter.top} y2=${phaseOuter.top + phasePlotHeight} className="grid-line" />
            <line x1=${x} x2=${x} y1=${phaseOuter.top + phasePlotHeight} y2=${phaseOuter.top + phasePlotHeight + 5} className="axis-line" />
            <text x=${x} y=${phaseHeight - 6} textAnchor="middle" className="axis-label">${labels[idx]}</text>
          </g>
        `;
      });
    }
    function intervalBands(top, plotHeight) {
      return allIntervalGroups.map(function (group) {
        const leftOffset = Math.min(group.start_offset_min, group.end_offset_min);
        const rightOffset = Math.max(group.start_offset_min, group.end_offset_min);
        const left = xMap(leftOffset);
        const right = xMap(rightOffset);
        const hovered = hoveredGroupId === group.group_id;
        return html`
          <g key=${top + "-interval-" + group.group_id} className=${hovered ? "interval-group hovered" : "interval-group"}>
            <rect x=${left} y=${top} width=${Math.max(right - left, 1.5)} height=${plotHeight} className="interval-band" />
            <line x1=${left} x2=${left} y1=${top} y2=${top + plotHeight} className="interval-boundary interval-start" />
            <line x1=${right} x2=${right} y1=${top} y2=${top + plotHeight} className="interval-boundary interval-end" />
          </g>
        `;
      });
    }
    function deleteIntervalGroup(groupId) {
      if (String(groupId).indexOf("staged:") === 0) {
        props.onDeletePendingInterval(groupId);
      } else {
        props.onDeleteInterval(groupId);
      }
    }
    function intervalDeleteButtons(top) {
      const buttonSize = 15;
      return allIntervalGroups.map(function (group) {
        const leftOffset = Math.min(group.start_offset_min, group.end_offset_min);
        const rightOffset = Math.max(group.start_offset_min, group.end_offset_min);
        const left = xMap(leftOffset);
        const right = xMap(rightOffset);
        const x = clamp(right - buttonSize - 2, left + 1, ampOuter.left + plotWidth - buttonSize - 1);
        const y = top + 2;
        const hovered = hoveredGroupId === group.group_id;
        return html`
          <g
            key=${top + "-interval-delete-" + group.group_id}
            className=${hovered ? "interval-delete hovered" : "interval-delete"}
            onMouseEnter=${function () {
              setHoveredGroupId(group.group_id);
            }}
            onMouseLeave=${function () {
              setHoveredGroupId(function (current) {
                return current === group.group_id ? null : current;
              });
            }}
            onPointerDown=${function (event) {
              event.stopPropagation();
            }}
            onClick=${function (event) {
              event.stopPropagation();
              deleteIntervalGroup(group.group_id);
            }}
          >
            <rect x=${x} y=${y} width=${buttonSize} height=${buttonSize} rx="3" ry="3" className="interval-delete-bg" />
            <text x=${x + buttonSize / 2} y=${y + buttonSize / 2 + 0.5} textAnchor="middle" dominantBaseline="middle" className="interval-delete-label">
              ×
            </text>
          </g>
        `;
      });
    }
    function crosshair(top, plotHeight) {
      if (!Number.isFinite(hoverOffset)) {
        return null;
      }
      const x = xMap(hoverOffset);
      return html`<line x1=${x} x2=${x} y1=${top} y2=${top + plotHeight} className="crosshair-line" />`;
    }
    function pendingAnchorLine(top, plotHeight) {
      if (!Number.isFinite(pendingAnchorJd)) {
        return null;
      }
      const offset = (pendingAnchorJd - startJd) * 1440.0;
      const x = xMap(offset);
      return html`<line x1=${x} x2=${x} y1=${top} y2=${top + plotHeight} className="anchor-line" />`;
    }
    function dragBand(top, plotHeight) {
      if (!dragState) {
        return null;
      }
      const left = xMap(Math.min(dragState.startOffset, dragState.currentOffset));
      const right = xMap(Math.max(dragState.startOffset, dragState.currentOffset));
      return html`<rect x=${left} y=${top} width=${Math.max(right - left, 1.5)} height=${plotHeight} className="drag-band" />`;
    }
    function pointerDown(event, svgRef, svgWidth) {
      if (props.busy) {
        return;
      }
      focusShell();
      const offset = updateHover(event, svgRef, svgWidth);
      if (!Number.isFinite(offset)) {
        return;
      }
      setPendingAnchorJd(null);
      setDragState({
        startOffset: offset,
        currentOffset: offset,
        startClientX: event.clientX,
        currentClientX: event.clientX,
      });
      if (event.currentTarget && event.currentTarget.setPointerCapture) {
        event.currentTarget.setPointerCapture(event.pointerId);
      }
      if (props.onStatus) {
        props.onStatus("Drag to stage flagged range. Click Apply Mask to apply.");
      }
    }
    function pointerMove(event, svgRef, svgWidth) {
      const offset = updateHover(event, svgRef, svgWidth);
      if (!Number.isFinite(offset)) {
        return;
      }
      setDragState(function (current) {
        if (!current) {
          return current;
        }
        return Object.assign({}, current, {
          currentOffset: offset,
          currentClientX: event.clientX,
        });
      });
    }
    function pointerUp(event, svgRef, svgWidth) {
      const offsetRaw = updateHover(event, svgRef, svgWidth);
      const offset = Number.isFinite(offsetRaw) ? offsetRaw : dragState ? dragState.currentOffset : null;
      if (event.currentTarget && event.currentTarget.releasePointerCapture) {
        try {
          event.currentTarget.releasePointerCapture(event.pointerId);
        } catch (_err) {}
      }
      if (!dragState || !Number.isFinite(offset)) {
        setDragState(null);
        return;
      }
      const start = offsetToJd(dragState.startOffset);
      const end = offsetToJd(offset);
      const dragWidthPx = Math.abs((Number.isFinite(event.clientX) ? event.clientX : dragState.currentClientX) - dragState.startClientX);
      setDragState(null);
      if (dragWidthPx < TIME_FLAG_MIN_DRAG_PX || Math.abs(end - start) <= 1.0e-9) {
        if (props.onStatus) {
          props.onStatus("Ignored click without drag.");
        }
        return;
      }
      props.onStageInterval(start, end);
    }
    function pointerLeave() {
      if (!dragState) {
        setHoverOffset(null);
        setHoverJd(null);
        setHoveredGroupId(null);
      }
    }
    function deleteHoveredInterval() {
      if (!hoveredGroupId) {
        if (props.onStatus) {
          props.onStatus("X ignored. Hover an interval first.");
        }
        return;
      }
      deleteIntervalGroup(hoveredGroupId);
    }
    function handleKeyDown(event) {
      const target = event.target;
      if (target && target.tagName && /^(INPUT|TEXTAREA|SELECT|BUTTON)$/.test(target.tagName)) {
        return;
      }
      const key = String(event.key || "").toUpperCase();
      if (key === "A") {
        event.preventDefault();
        if (!Number.isFinite(hoverJd)) {
          if (props.onStatus) {
            props.onStatus("A ignored. Hover inside Time History first.");
          }
          return;
        }
        setPendingAnchorJd(hoverJd);
        if (props.onStatus) {
          props.onStatus("A anchor set at " + formatUtcTime(hoverJd) + " UT.");
        }
      } else if (key === "B") {
        event.preventDefault();
        if (!Number.isFinite(pendingAnchorJd) || !Number.isFinite(hoverJd)) {
          if (props.onStatus) {
            props.onStatus("B ignored. Set A first, then hover a second time.");
          }
          return;
        }
        props.onStageInterval(pendingAnchorJd, hoverJd);
        setPendingAnchorJd(null);
      } else if (key === "X") {
        event.preventDefault();
        deleteHoveredInterval();
      } else if (key === "ESCAPE") {
        event.preventDefault();
        setPendingAnchorJd(null);
        if (props.onStatus) {
          props.onStatus("Cleared pending A anchor.");
        }
      }
    }

    return html`
      <div className="time-history-shell compact-time-history-shell" ref=${wrapperRef} tabIndex="0" onKeyDown=${handleKeyDown}>
        <div className="time-history-toolbar">
          <div className="scope-pills" role="group" aria-label="Time-flag scope">
            ${TIME_FLAG_SCOPES.map(function (item) {
              return html`
                <button
                  type="button"
                  key=${item.id}
                  className=${"scope-pill" + (props.scope === item.id ? " active" : "")}
                  disabled=${props.busy}
                  onClick=${function () {
                    props.onScopeChange(item.id);
                  }}
                >
                  ${item.label}
                </button>
              `;
            })}
          </div>
        </div>
        <svg
          ref=${ampSvgRef}
          viewBox=${"0 0 " + width + " " + ampHeight}
          className="svg-plot"
          preserveAspectRatio="xMidYMid meet"
          style=${{ aspectRatio: width + " / " + ampHeight }}
          role="img"
          aria-label=${"Time history amplitude " + data.title}
          onMouseEnter=${focusShell}
        >
          ${ampTickLines()}
          ${intervalBands(ampOuter.top, ampPlotHeight)}
          ${dragBand(ampOuter.top, ampPlotHeight)}
          ${crosshair(ampOuter.top, ampPlotHeight)}
          ${pendingAnchorLine(ampOuter.top, ampPlotHeight)}
          <rect x=${ampOuter.left} y=${ampOuter.top} width=${plotWidth} height=${ampPlotHeight} className="plot-frame" />
          <text x=${ampOuter.left + 6} y=${ampOuter.top + 12} className="panel-title">Amplitude</text>
          <text
            x="16"
            y=${ampOuter.top + ampPlotHeight / 2}
            transform=${"rotate(-90 16 " + (ampOuter.top + ampPlotHeight / 2) + ")"}
            className="axis-label"
          >
            Amp
          </text>
          ${(data.series || []).map(function (series) {
            return pointElements(series, "amp", ampMap);
          })}
          <rect
            x=${ampOuter.left}
            y=${ampOuter.top}
            width=${plotWidth}
            height=${ampPlotHeight}
            className="time-history-hitbox"
            onPointerDown=${function (event) {
              pointerDown(event, ampSvgRef, width);
            }}
            onPointerMove=${function (event) {
              pointerMove(event, ampSvgRef, width);
            }}
            onPointerUp=${function (event) {
              pointerUp(event, ampSvgRef, width);
            }}
            onPointerLeave=${pointerLeave}
          />
          ${intervalDeleteButtons(ampOuter.top)}
        </svg>
        <svg
          ref=${phaseSvgRef}
          viewBox=${"0 0 " + width + " " + phaseHeight}
          className="svg-plot"
          preserveAspectRatio="xMidYMid meet"
          style=${{ aspectRatio: width + " / " + phaseHeight }}
          role="img"
          aria-label=${"Time history phase " + data.title}
          onMouseEnter=${focusShell}
        >
          ${phaseTickLines()}
          ${phaseXTicks()}
          ${intervalBands(phaseOuter.top, phasePlotHeight)}
          ${dragBand(phaseOuter.top, phasePlotHeight)}
          ${crosshair(phaseOuter.top, phasePlotHeight)}
          ${pendingAnchorLine(phaseOuter.top, phasePlotHeight)}
          <rect x=${phaseOuter.left} y=${phaseOuter.top} width=${plotWidth} height=${phasePlotHeight} className="plot-frame" />
          <text x=${phaseOuter.left + 6} y=${phaseOuter.top + 12} className="panel-title">Phase</text>
          <text
            x="16"
            y=${phaseOuter.top + phasePlotHeight / 2}
            transform=${"rotate(-90 16 " + (phaseOuter.top + phasePlotHeight / 2) + ")"}
            className="axis-label"
          >
            Phase
          </text>
          <text x=${phaseOuter.left + plotWidth / 2} y=${phaseHeight - 6} textAnchor="middle" className="axis-label">Time [UT]</text>
          ${(data.series || []).map(function (series) {
            return pointElements(series, "phase", phaseMap);
          })}
          <rect
            x=${phaseOuter.left}
            y=${phaseOuter.top}
            width=${plotWidth}
            height=${phasePlotHeight}
            className="time-history-hitbox"
            onPointerDown=${function (event) {
              pointerDown(event, phaseSvgRef, width);
            }}
            onPointerMove=${function (event) {
              pointerMove(event, phaseSvgRef, width);
            }}
            onPointerUp=${function (event) {
              pointerUp(event, phaseSvgRef, width);
            }}
            onPointerLeave=${pointerLeave}
          />
        </svg>
      </div>
    `;
  }

  function App() {
    const [sessionId, setSessionId] = useState(null);
    const [state, setState] = useState(null);
    const [dateText, setDateText] = useState(todayIso());
    const [requestedDateText, setRequestedDateText] = useState(null);
    const [checkedScanIds, setCheckedScanIds] = useState([]);
    const [scanSelectionAnchor, setScanSelectionAnchor] = useState(null);
    const [dataRevision, setDataRevision] = useState(0);
    const [selectionRevision, setSelectionRevision] = useState(0);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState("");
    const [interactionMessage, setInteractionMessage] = useState("");
    const [timeFlagScope, setTimeFlagScope] = useState("selected");
    const [inbandAntennaScope, setInbandAntennaScope] = useState("selected");
    const [inbandPolScope, setInbandPolScope] = useState("selected");
    const [delayDraft, setDelayDraft] = useState({ ant: 1, x: "", y: "" });
    const [relativeDelayDraft, setRelativeDelayDraft] = useState({ ant: 1, x: "", y: "" });
    const [yxThresholdDraft, setYxThresholdDraft] = useState(String(1.5));
    const [residualThresholdDraft, setResidualThresholdDraft] = useState(String(1.0));
    const [timeHistoryData, setTimeHistoryData] = useState(null);
    const [heatmapData, setHeatmapData] = useState(null);
    const [overviewData, setOverviewData] = useState(null);
    const [stagedTimeIntervals, setStagedTimeIntervals] = useState([]);
    const [stagedInbandPanels, setStagedInbandPanels] = useState({});
    const [stagedInbandMasks, setStagedInbandMasks] = useState({});
    const [stagedResidualPanels, setStagedResidualPanels] = useState({});
    const [stagedResidualMasks, setStagedResidualMasks] = useState({});
    const [heatmapLoadedRevision, setHeatmapLoadedRevision] = useState(-1);
    const [timeHistoryLoadedRevision, setTimeHistoryLoadedRevision] = useState(-1);
    const [overviewLoadedRevision, setOverviewLoadedRevision] = useState(-1);
    const [pendingProgressLoad, setPendingProgressLoad] = useState(null);
    const [activity, setActivity] = useState(null);
    const [inbandResidualInspector, setInbandResidualInspector] = useState(null);
    const [paneMaxHeight, setPaneMaxHeight] = useState(null);
    const [toolbarCollapsed, setToolbarCollapsed] = useState(function () {
      if (typeof window === "undefined") {
        return false;
      }
      try {
        return window.localStorage.getItem("calwidget_v2_toolbar_collapsed") === "1";
      } catch (_err) {
        return false;
      }
    });
    const progressTimerRef = useRef(null);
    const finishTimerRef = useRef(null);
    const shellRef = useRef(null);
    const toolbarRef = useRef(null);

    function clearActivityTimers() {
      if (progressTimerRef.current) {
        window.clearInterval(progressTimerRef.current);
        progressTimerRef.current = null;
      }
      if (finishTimerRef.current) {
        window.clearTimeout(finishTimerRef.current);
        finishTimerRef.current = null;
      }
    }

    useEffect(function () {
      return function () {
        clearActivityTimers();
      };
    }, []);

    useEffect(
      function () {
        if (!pendingProgressLoad) {
          return;
        }
        const loadedCount = [
          heatmapLoadedRevision >= pendingProgressLoad.revision,
          timeHistoryLoadedRevision >= pendingProgressLoad.revision,
          overviewLoadedRevision >= pendingProgressLoad.revision,
        ].filter(Boolean).length;
        setActivity(function (current) {
          if (!current || current.kind !== pendingProgressLoad.kind) {
            return current;
          }
          const progress = loadedCount >= 3 ? current.progress : 94 + loadedCount * 2;
          return Object.assign({}, current, {
            progress: Math.max(current.progress, progress),
            stage: loadedCount > 0 && loadedCount < 3
              ? "Loading plots and refreshed products (" + String(loadedCount) + "/3)"
              : current.stage,
          });
        });
        if (
          heatmapLoadedRevision < pendingProgressLoad.revision ||
          timeHistoryLoadedRevision < pendingProgressLoad.revision ||
          overviewLoadedRevision < pendingProgressLoad.revision
        ) {
          return;
        }
        finishProgressActivity(pendingProgressLoad.kind, pendingProgressLoad.message, false);
        setPendingProgressLoad(null);
        setBusy(false);
      },
      [pendingProgressLoad, heatmapLoadedRevision, timeHistoryLoadedRevision, overviewLoadedRevision]
    );

    useEffect(function () {
      jsonFetch("/api/session", { method: "POST" })
        .then(function (data) {
          setSessionId(data.session_id);
        })
        .catch(function (err) {
          setError(err.message);
        });
    }, []);

    useEffect(
      function () {
        if (typeof window === "undefined") {
          return;
        }
        try {
          window.localStorage.setItem("calwidget_v2_toolbar_collapsed", toolbarCollapsed ? "1" : "0");
        } catch (_err) {}
      },
      [toolbarCollapsed]
    );

    useEffect(
      function () {
        function updatePaneMaxHeight() {
          if (!shellRef.current || !toolbarRef.current || typeof window === "undefined") {
            return;
          }
          const shellStyle = window.getComputedStyle(shellRef.current);
          const paddingTop = parseFloat(shellStyle.paddingTop) || 0;
          const paddingBottom = parseFloat(shellStyle.paddingBottom) || 0;
          const gap = parseFloat(shellStyle.rowGap || shellStyle.gap) || 0;
          const toolbarHeight = toolbarRef.current.getBoundingClientRect().height || 0;
          const available = Math.floor(window.innerHeight - paddingTop - paddingBottom - gap - toolbarHeight);
          setPaneMaxHeight(Math.max(260, available));
        }
        updatePaneMaxHeight();
        window.addEventListener("resize", updatePaneMaxHeight);
        let resizeObserver = null;
        if (typeof ResizeObserver !== "undefined" && toolbarRef.current) {
          resizeObserver = new ResizeObserver(updatePaneMaxHeight);
          resizeObserver.observe(toolbarRef.current);
        }
        return function () {
          window.removeEventListener("resize", updatePaneMaxHeight);
          if (resizeObserver) {
            resizeObserver.disconnect();
          }
        };
      },
      []
    );

    useEffect(
      function () {
        setInbandResidualInspector(null);
        setStagedTimeIntervals([]);
        setStagedInbandPanels({});
        setStagedInbandMasks({});
        setStagedResidualPanels({});
        setStagedResidualMasks({});
      },
      [dataRevision]
    );

    function syncDraft(nextState) {
      const selectedAnt = nextState ? nextState.selected_ant : 0;
      const active = nextState ? nextState.active_refcal : null;
      setDelayDraft({
        ant: selectedAnt + 1,
        x: active && active.x_delay_ns !== null ? String(active.x_delay_ns.toFixed(3)) : "",
        y: active && active.y_delay_ns !== null ? String(active.y_delay_ns.toFixed(3)) : "",
      });
      setRelativeDelayDraft({
        ant: selectedAnt + 1,
        x:
          active && active.x_applied_relative_delay_ns !== null
            ? String(active.x_applied_relative_delay_ns.toFixed(3))
            : active && active.x_relative_delay_ns !== null
              ? String(active.x_relative_delay_ns.toFixed(3))
              : "",
        y:
          active && active.y_applied_relative_delay_ns !== null
            ? String(active.y_applied_relative_delay_ns.toFixed(3))
            : active && active.y_relative_delay_ns !== null
              ? String(active.y_relative_delay_ns.toFixed(3))
              : "",
      });
      setYxThresholdDraft(
        active && active.yx_residual_threshold_rad !== null && active.yx_residual_threshold_rad !== undefined
          ? String(Number(active.yx_residual_threshold_rad).toFixed(2))
          : String(1.5)
      );
      setResidualThresholdDraft(
        active && active.residual_band_threshold_rad !== null && active.residual_band_threshold_rad !== undefined
          ? String(Number(active.residual_band_threshold_rad).toFixed(2))
          : String(1.0)
      );
    }

    async function refresh() {
      if (!sessionId) {
        return;
      }
      const next = await jsonFetch("/api/state?session_id=" + encodeURIComponent(sessionId));
      setState(next);
      setInteractionMessage("");
      syncDraft(next);
      setDataRevision(function (value) {
        return value + 1;
      });
      setSelectionRevision(function (value) {
        return value + 1;
      });
    }

    function startProgressActivity(kind) {
      const profile = ACTION_PROGRESS[kind];
      if (!profile) {
        return;
      }
      clearActivityTimers();
      const startedAt = Date.now();
      setActivity({
        kind: kind,
        title: profile.label,
        stage: profile.stages[0],
        progress: 4,
        error: false,
      });
      progressTimerRef.current = window.setInterval(function () {
        const elapsed = Date.now() - startedAt;
        setActivity(function (current) {
          if (!current || current.kind !== kind) {
            return current;
          }
          const progress = Math.min(92, 4 + (elapsed / profile.paceMs) * 88);
          const stageIdx = Math.min(profile.stages.length - 1, Math.floor((progress - 4) / 24));
          return Object.assign({}, current, {
            progress: progress,
            stage: profile.stages[stageIdx],
          });
        });
      }, 160);
    }

    function finishProgressActivity(kind, message, isError) {
      clearActivityTimers();
      setActivity(function (current) {
        if (!current || current.kind !== kind) {
          return current;
        }
        return Object.assign({}, current, {
          progress: isError ? current.progress : 100,
          stage: message,
          error: !!isError,
        });
      });
      finishTimerRef.current = window.setTimeout(function () {
        setActivity(null);
      }, isError ? 2800 : 900);
    }

    async function runAction(fn, options) {
      setBusy(true);
      setError("");
      const progressKind = options && options.progressKind ? options.progressKind : null;
      const progressProfile = progressKind ? ACTION_PROGRESS[progressKind] : null;
      let deferBusyClear = false;
      if (progressProfile) {
        startProgressActivity(progressKind);
      }
      try {
        await fn();
        if (progressProfile && progressProfile.waitForPlots) {
          setActivity(function (current) {
            if (!current || current.kind !== progressKind) {
              return current;
            }
            return Object.assign({}, current, {
              progress: Math.max(current.progress, 94),
              stage: "Loading plots and refreshed products",
            });
          });
          setPendingProgressLoad({
            kind: progressKind,
            revision: dataRevision + 1,
            message: (options && options.successMessage) || progressProfile.success,
          });
          deferBusyClear = true;
        } else if (progressProfile) {
          finishProgressActivity(progressKind, (options && options.successMessage) || progressProfile.success, false);
        }
      } catch (err) {
        setPendingProgressLoad(null);
        if (progressProfile) {
          finishProgressActivity(progressKind, err.message || String(err), true);
        }
        setError(err.message || String(err));
      } finally {
        if (!deferBusyClear) {
          setBusy(false);
        }
      }
    }

    useEffect(
      function () {
        if (!sessionId) {
          return;
        }
        refresh().catch(function () {});
      },
      [sessionId]
    );

    useEffect(
      function () {
        if (!sessionId || busy) {
          return;
        }
        if (!/^\d{4}-\d{2}-\d{2}$/.test(dateText)) {
          return;
        }
        if (dateText === requestedDateText) {
          return;
        }
        setRequestedDateText(dateText);
        runAction(function () {
          return loadDate(dateText);
        });
      },
      [sessionId, dateText, busy, requestedDateText]
    );

    useEffect(
      function () {
        if (!sessionId) {
          return;
        }
        let cancelled = false;
        const revision = dataRevision;
        setHeatmapData(null);
        jsonFetch("/api/plot/heatmap-data?session_id=" + encodeURIComponent(sessionId))
          .then(function (data) {
            if (!cancelled) {
              setHeatmapData(data);
              setHeatmapLoadedRevision(revision);
            }
          })
          .catch(function (err) {
            if (!cancelled) {
              setHeatmapData({ message: err.message || String(err) });
              setHeatmapLoadedRevision(revision);
            }
          });
        return function () {
          cancelled = true;
        };
      },
      [sessionId, dataRevision]
    );

    useEffect(
      function () {
        if (!sessionId) {
          return;
        }
        let cancelled = false;
        const revision = dataRevision;
        setTimeHistoryData(null);
        jsonFetch("/api/plot/time-history?session_id=" + encodeURIComponent(sessionId))
          .then(function (data) {
            if (!cancelled) {
              setTimeHistoryData(data);
              setTimeHistoryLoadedRevision(revision);
            }
          })
          .catch(function (err) {
            if (!cancelled) {
              setTimeHistoryData({ message: err.message || String(err) });
              setTimeHistoryLoadedRevision(revision);
            }
          });
        return function () {
          cancelled = true;
        };
      },
      [sessionId, dataRevision, selectionRevision]
    );

    useEffect(
      function () {
        if (!sessionId) {
          return;
        }
        let cancelled = false;
        const revision = dataRevision;
        setOverviewData(null);
        jsonFetch("/api/plot/overview-data?session_id=" + encodeURIComponent(sessionId))
          .then(function (data) {
            if (!cancelled) {
              setOverviewData(data);
              setOverviewLoadedRevision(revision);
            }
          })
          .catch(function (err) {
            if (!cancelled) {
              setOverviewData({
                sum_amp: { message: err.message || String(err), title: "Sum Amp" },
                sum_pha: { message: err.message || String(err), title: "Sum Pha" },
                inband_fit: { message: err.message || String(err), title: "Inband Fit" },
                inband_residual_phase_band: { message: err.message || String(err), title: "Per-Band Residual Phase" },
                inband_relative_phase: { message: err.message || String(err), title: "Relative Phase + Fit" },
                inband_residual_delay_band: { message: err.message || String(err), title: "Residual Delay Per Band" },
              });
              setOverviewLoadedRevision(revision);
            }
          });
        return function () {
          cancelled = true;
        };
      },
      [sessionId, dataRevision]
    );

    async function loadDate(dateOverride) {
      if (!sessionId) {
        return;
      }
      const targetDate = dateOverride || dateText;
      const next = await jsonFetch(
        "/api/scans?session_id=" + encodeURIComponent(sessionId) + "&date=" + encodeURIComponent(targetDate)
      );
      setState(next);
      setInteractionMessage("");
      syncDraft(next);
      setCheckedScanIds([]);
      setScanSelectionAnchor(next && next.selected_scan_id !== null ? next.selected_scan_id : null);
      setDataRevision(function (value) {
        return value + 1;
      });
      setSelectionRevision(function (value) {
        return value + 1;
      });
    }

    async function postJson(url, payload, options) {
      const next = await jsonFetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setState(next);
      setInteractionMessage("");
      syncDraft(next);
      if (options && options.selectionOnly) {
        setSelectionRevision(function (value) {
          return value + 1;
        });
        return next;
      }
      setDataRevision(function (value) {
        return value + 1;
      });
      setSelectionRevision(function (value) {
        return value + 1;
      });
      return next;
    }

    async function postJsonWithOverviewPatch(url, payload, antennaIndex) {
      const next = await jsonFetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const nextState = next && next.state ? next.state : next;
      setState(nextState);
      setInteractionMessage("");
      syncDraft(nextState);
      if (next && next.overview_updates) {
        const responseAntenna = next.updated_antenna;
        const patchAntenna = Number.isFinite(responseAntenna) ? responseAntenna : antennaIndex;
        setOverviewData(function (current) {
          return mergeOverviewAntennaUpdates(current, next.overview_updates, patchAntenna);
        });
      }
      if (next && next.heatmap) {
        setHeatmapData(next.heatmap);
      }
      return next;
    }

    async function postJsonWithOverviewRefresh(url, payload) {
      const next = await jsonFetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const nextState = next && next.state ? next.state : next;
      setState(nextState);
      setInteractionMessage("");
      syncDraft(nextState);
      if (next && next.overview) {
        setOverviewData(next.overview);
      }
      if (next && next.heatmap) {
        setHeatmapData(next.heatmap);
      }
      return next;
    }

    async function postJsonWithTimeFlagPatch(url, payload) {
      const next = await jsonFetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const nextState = next && next.state ? next.state : next;
      setState(nextState);
      setInteractionMessage("");
      syncDraft(nextState);
      if (next && next.overview_updates) {
        setOverviewData(function (current) {
          return mergeOverviewAntennaUpdates(current, next.overview_updates, null);
        });
      }
      if (next && next.heatmap) {
        setHeatmapData(next.heatmap);
      }
      if (next && next.time_history) {
        setTimeHistoryData(next.time_history);
      }
      return next;
    }

    function selectedScanId() {
      return state && state.selected_scan_id !== null ? state.selected_scan_id : null;
    }

    function orderedScanIds(scanIds) {
      const scans = state && state.scans ? state.scans : [];
      const order = new Map(
        scans.map(function (scan, idx) {
          return [scan.scan_id, idx];
        })
      );
      return Array.from(new Set(scanIds)).sort(function (left, right) {
        return (order.has(left) ? order.get(left) : Number.MAX_SAFE_INTEGER) - (order.has(right) ? order.get(right) : Number.MAX_SAFE_INTEGER);
      });
    }

    function scanRangeIds(anchorId, targetId) {
      const scans = state && state.scans ? state.scans : [];
      const anchorIndex = scans.findIndex(function (scan) {
        return scan.scan_id === anchorId;
      });
      const targetIndex = scans.findIndex(function (scan) {
        return scan.scan_id === targetId;
      });
      if (anchorIndex < 0 || targetIndex < 0) {
        return [targetId];
      }
      const start = Math.min(anchorIndex, targetIndex);
      const stop = Math.max(anchorIndex, targetIndex) + 1;
      return scans.slice(start, stop).map(function (scan) {
        return scan.scan_id;
      });
    }

    function onScanRowClick(event, scanId) {
      if (!sessionId) {
        return;
      }
      const isRange = !!(event && event.shiftKey);
      const isToggle = !!(event && (event.metaKey || event.ctrlKey));
      const anchorId = scanSelectionAnchor !== null ? scanSelectionAnchor : selectedScanId();
      if (isRange) {
        const nextSelection = anchorId !== null ? scanRangeIds(anchorId, scanId) : [scanId];
        setCheckedScanIds(orderedScanIds(nextSelection));
        setScanSelectionAnchor(anchorId !== null ? anchorId : scanId);
      } else if (isToggle) {
        setCheckedScanIds(function (current) {
          const exists = current.indexOf(scanId) >= 0;
          return orderedScanIds(
            exists
              ? current.filter(function (value) {
                  return value !== scanId;
                })
              : current.concat([scanId])
          );
        });
        setScanSelectionAnchor(scanId);
      } else {
        setCheckedScanIds([]);
        setScanSelectionAnchor(scanId);
      }
      runAction(function () {
        return postJson("/api/select-scan", { session_id: sessionId, scan_id: scanId });
      });
    }

    function updateSelection(ant, band) {
      if (!sessionId || !state) {
        return Promise.resolve();
      }
      const nextAnt = Math.max(0, ant);
      const nextBand = Math.max(0, band);
      if (state.selected_ant === nextAnt && state.selected_band === nextBand) {
        return Promise.resolve();
      }
      return postJson(
        "/api/select-band",
        {
          session_id: sessionId,
          antenna: nextAnt,
          band: nextBand,
        },
        { selectionOnly: true }
      );
    }

    function onHeatmapSelect(ant, band) {
      if (busy) {
        return;
      }
      if (state && state.selected_ant === ant && state.selected_band === band) {
        return;
      }
      runAction(function () {
        return updateSelection(ant, band);
      });
    }

    function stageTimeFlag(startJd, endJd) {
      if (!state) {
        return;
      }
      const normalized = normalizeJdInterval(startJd, endJd);
      if (Math.abs(normalized.end_jd - normalized.start_jd) <= 1.0e-9) {
        setInteractionMessage("Ignored click without drag.");
        return;
      }
      const pending = {
        temp_id: "staged:" + Date.now().toString(36) + ":" + Math.random().toString(36).slice(2, 8),
        antenna: Number(state.selected_ant),
        band: Number(state.selected_band),
        scope: String(timeFlagScope),
        start_jd: Number(normalized.start_jd),
        end_jd: Number(normalized.end_jd),
      };
      setStagedTimeIntervals(function (current) {
        return mergeTimeFlagIntervals(current, pending);
      });
      setInteractionMessage("Time flag staged. Click Apply Mask to apply.");
    }

    function removeStagedTimeFlag(groupId) {
      setStagedTimeIntervals(function (current) {
        return current.filter(function (item) {
          return String(item.temp_id) !== String(groupId);
        });
      });
      setInteractionMessage("Removed staged time flag.");
    }

    function applyStagedTimeFlags() {
      if (busy || !sessionId || !stagedTimeIntervals.length) {
        return;
      }
      setInteractionMessage("");
      runAction(
        function () {
          return postJsonWithTimeFlagPatch("/api/time-flags/add-batch", {
            session_id: sessionId,
            intervals: stagedTimeIntervals.map(function (item) {
              return {
                antenna: Number(item.antenna),
                band: Number(item.band),
                start_jd: Number(item.start_jd),
                end_jd: Number(item.end_jd),
                scope: String(item.scope),
              };
            }),
          }).then(function (next) {
            setStagedTimeIntervals([]);
            return next;
          });
        },
        { progressKind: "time_flag", successMessage: "Time flag applied" }
      );
    }

    function deleteTimeFlag(groupId) {
      setInteractionMessage("");
      runAction(
        function () {
          return postJsonWithTimeFlagPatch("/api/time-flags/delete", {
            session_id: sessionId,
            group_id: groupId,
          });
        },
        { progressKind: "time_flag", successMessage: "Time flag removed" }
      );
    }

    function targetInbandRows(sourcePol) {
      const sourceRow = Number(sourcePol);
      if (sourceRow === 2) {
        return [0, 1];
      }
      const pol = clamp(sourceRow, 0, 1);
      return inbandPolScope === "all" ? [0, 1] : [pol];
    }

    function targetInbandPanels(sourcePol, sourceAntenna) {
      const panelIdx = Math.max(0, Number(sourceAntenna));
      const baseSection = (overviewData && (overviewData.inband_fit || overviewData.inband_relative_phase)) || null;
      const targets = [];
      targetInbandRows(sourcePol).forEach(function (rowIdx) {
        const row = baseSection && baseSection.panels && baseSection.panels[rowIdx] ? baseSection.panels[rowIdx] : [];
        if (inbandAntennaScope === "all") {
          row.forEach(function (_panel, idx) {
            targets.push({ rowIdx: rowIdx, panelIdx: idx });
          });
        } else if (row[panelIdx]) {
          targets.push({ rowIdx: rowIdx, panelIdx: panelIdx });
        }
      });
      return targets;
    }

    function stageInbandSelection(startBand, endBand, sourcePol, sourceAntenna, mode) {
      if (!overviewData) {
        return false;
      }
      const sectionIds = ["inband_fit", "inband_relative_phase"];
      const targets = targetInbandPanels(sourcePol, sourceAntenna);
      const nextOverrides = {};
      const nextMasks = {};
      sectionIds.forEach(function (sectionId) {
        const section = overviewData[sectionId];
        const bandEdges = section && section.band_edges ? section.band_edges : [];
        targets.forEach(function (target) {
          const originalPanel = section && section.panels && section.panels[target.rowIdx] ? section.panels[target.rowIdx][target.panelIdx] : null;
          if (!originalPanel || !bandEdges.length) {
            return;
          }
          const key = panelSelectionKey(sectionId, target.rowIdx, target.panelIdx);
          const currentPanel = stagedInbandPanels[key]
            ? Object.assign({}, originalPanel, stagedInbandPanels[key])
            : originalPanel;
          const nextRanges = updateKeptRanges(currentPanel.kept_ranges, bandEdges, startBand, endBand, mode);
          nextOverrides[key] = optimisticPanelUpdate(currentPanel, nextRanges, bandEdges);
          nextMasks[targetMaskKey(target.rowIdx, target.panelIdx)] = {
            antenna: target.panelIdx,
            polarization: target.rowIdx,
            kept_ranges: nextRanges,
          };
        });
      });
      const keys = Object.keys(nextOverrides);
      if (keys.length) {
        setStagedInbandPanels(function (current) {
          return Object.assign({}, current, nextOverrides);
        });
        setStagedInbandMasks(function (current) {
          return Object.assign({}, current, nextMasks);
        });
        setInteractionMessage("Flag mask staged. Click Apply Mask to apply.");
      }
      return keys.length > 0;
    }

    function stageInbandMaskClear(rowIdx, panelIdx) {
      if (!overviewData) {
        return false;
      }
      const sectionIds = ["inband_fit", "inband_relative_phase"];
      const nextOverrides = {};
      const nextMasks = {};
      targetInbandRows(rowIdx).forEach(function (maskRowIdx) {
        sectionIds.forEach(function (sectionId) {
          const section = overviewData[sectionId];
          const bandEdges = section && section.band_edges ? section.band_edges : [];
          const row = section && section.panels && section.panels[maskRowIdx] ? section.panels[maskRowIdx] : [];
          const originalPanel = row[panelIdx];
          if (!originalPanel || !bandEdges.length) {
            return;
          }
          const nextRanges = defaultKeptRanges(bandEdges);
          const key = panelSelectionKey(sectionId, maskRowIdx, panelIdx);
          nextOverrides[key] = optimisticPanelUpdate(originalPanel, nextRanges, bandEdges);
        });
      });
      const resetRanges = sectionIds.length ? defaultKeptRanges((overviewData.inband_fit && overviewData.inband_fit.band_edges) || []) : [];
      targetInbandRows(rowIdx).forEach(function (maskRowIdx) {
        nextMasks[targetMaskKey(maskRowIdx, panelIdx)] = {
          antenna: panelIdx,
          polarization: maskRowIdx,
          kept_ranges: resetRanges,
        };
      });
      if (Object.keys(nextOverrides).length) {
        setStagedInbandPanels(function (current) {
          return Object.assign({}, current, nextOverrides);
        });
        setStagedInbandMasks(function (current) {
          return Object.assign({}, current, nextMasks);
        });
        setInteractionMessage("Mask cleared for this antenna/polarization. Click Apply Mask to apply.");
        return true;
      }
      return false;
    }

    function clearStagedInbandSelection() {
      setStagedInbandPanels({});
      setStagedInbandMasks({});
    }

    function clearStagedResidualSelection() {
      setStagedResidualPanels({});
      setStagedResidualMasks({});
    }

    function stageResidualSelection(startBand, endBand, rowIdx, panelIdx) {
      if (!overviewData) {
        return false;
      }
      const section = overviewData.inband_residual_phase_band;
      const bandEdges = section && section.band_edges ? section.band_edges : [];
      const row = section && section.panels && section.panels[rowIdx] ? section.panels[rowIdx] : [];
      const originalPanel = row[panelIdx];
      if (!originalPanel || !bandEdges.length) {
        return false;
      }
      const key = panelSelectionKey("inband_residual_phase_band", rowIdx, panelIdx);
      const currentPanel = stagedResidualPanels[key]
        ? Object.assign({}, originalPanel, stagedResidualPanels[key])
        : originalPanel;
      const nextRanges = excludeRangeFromKeptRanges(currentPanel.kept_ranges, bandEdges, startBand, endBand);
      setStagedResidualPanels(function (current) {
        return Object.assign({}, current, {
          [key]: optimisticPanelUpdate(currentPanel, nextRanges, bandEdges),
        });
      });
      setStagedResidualMasks(function (current) {
        return Object.assign({}, current, {
          [targetMaskKey(rowIdx, panelIdx)]: {
            antenna: panelIdx,
            polarization: rowIdx,
            kept_ranges: nextRanges,
          },
        });
      });
      setInteractionMessage("Residual mask staged. Click Apply Residual Fit to apply.");
      return true;
    }

    function stageResidualMaskClear(rowIdx, panelIdx) {
      if (!overviewData) {
        return false;
      }
      const section = overviewData.inband_residual_phase_band;
      const bandEdges = section && section.band_edges ? section.band_edges : [];
      const row = section && section.panels && section.panels[rowIdx] ? section.panels[rowIdx] : [];
      const originalPanel = row[panelIdx];
      if (!originalPanel || !bandEdges.length) {
        return false;
      }
      const nextRanges =
        originalPanel.auto_kept_ranges && originalPanel.auto_kept_ranges.length
          ? originalPanel.auto_kept_ranges
          : defaultKeptRanges(bandEdges);
      const key = panelSelectionKey("inband_residual_phase_band", rowIdx, panelIdx);
      setStagedResidualPanels(function (current) {
        return Object.assign({}, current, {
          [key]: optimisticPanelUpdate(originalPanel, nextRanges, bandEdges),
        });
      });
      setStagedResidualMasks(function (current) {
        return Object.assign({}, current, {
          [targetMaskKey(rowIdx, panelIdx)]: {
            antenna: panelIdx,
            polarization: rowIdx,
            kept_ranges: nextRanges,
          },
        });
      });
      setInteractionMessage("Residual mask reset to the auto mask. Click Apply Residual Fit to apply.");
      return true;
    }

    function panelWithStagedResidualSelection(rowIdx, panelIdx, panel) {
      const override = stagedResidualPanels[panelSelectionKey("inband_residual_phase_band", rowIdx, panelIdx)];
      return override ? Object.assign({}, panel, override) : panel;
    }

    function panelWithStagedInbandSelection(sectionId, rowIdx, panelIdx, panel) {
      if (sectionId === "inband_relative_phase" && rowIdx === 2) {
        const section = overviewData && overviewData[sectionId] ? overviewData[sectionId] : null;
        const bandEdges = section && section.band_edges ? section.band_edges : [];
        const xKey = panelSelectionKey(sectionId, 0, panelIdx);
        const yKey = panelSelectionKey(sectionId, 1, panelIdx);
        const xRow = section && section.panels && section.panels[0] ? section.panels[0] : [];
        const yRow = section && section.panels && section.panels[1] ? section.panels[1] : [];
        const xPanel = stagedInbandPanels[xKey]
          ? Object.assign({}, xRow[panelIdx] || {}, stagedInbandPanels[xKey])
          : xRow[panelIdx];
        const yPanel = stagedInbandPanels[yKey]
          ? Object.assign({}, yRow[panelIdx] || {}, stagedInbandPanels[yKey])
          : yRow[panelIdx];
        if (xPanel && yPanel && bandEdges.length) {
          return Object.assign({}, panel, {
            kept_ranges: intersectKeptRanges(xPanel.kept_ranges, yPanel.kept_ranges, bandEdges),
          });
        }
      }
      const override = stagedInbandPanels[panelSelectionKey(sectionId, rowIdx, panelIdx)];
      return override ? Object.assign({}, panel, override) : panel;
    }

    function stageInbandWindow(startBand, endBand, sourcePol, sourceAntenna, mode) {
      if (busy) {
        return;
      }
      stageInbandSelection(startBand, endBand, sourcePol, sourceAntenna, mode);
    }

    function applyStagedInbandWindow() {
      const targets = Object.values(stagedInbandMasks);
      if (busy || !targets.length) {
        return;
      }
      setInteractionMessage("");
      runAction(
        function () {
          return jsonFetch("/api/inband/mask/batch", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              session_id: sessionId,
              targets: targets,
            }),
          }).then(function (next) {
            const nextState = next && next.state ? next.state : next;
            setState(nextState);
            setInteractionMessage("");
            syncDraft(nextState);
            if (next && next.overview_updates) {
              setOverviewData(function (current) {
                return mergeOverviewAntennaUpdates(current, next.overview_updates, null);
              });
            }
            clearStagedInbandSelection();
            return next;
          });
        },
        { progressKind: "inband_mask", successMessage: "Mask applied" }
      );
    }

    function applyDelayEditorUpdate() {
      const antennaIndex = Math.max(0, parseInt(delayDraft.ant || "1", 10) - 1);
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/inband/update",
            {
              session_id: sessionId,
              antenna: antennaIndex,
              x_delay_ns: delayDraft.x === "" ? null : parseFloat(delayDraft.x),
              y_delay_ns: delayDraft.y === "" ? null : parseFloat(delayDraft.y),
            },
            antennaIndex
          );
        },
        { progressKind: "active_delay", successMessage: "In-band delay updated" }
      );
    }

    function resetDelayEditorAntenna() {
      const antennaIndex = Math.max(0, parseInt(delayDraft.ant || "1", 10) - 1);
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/inband/reset",
            {
              session_id: sessionId,
              antenna: antennaIndex,
            },
            antennaIndex
          );
        },
        { progressKind: "active_delay", successMessage: "In-band delay reset" }
      );
    }

    function resetAllDelayEditor() {
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/inband/reset",
            {
              session_id: sessionId,
              antenna: null,
            },
            null
          );
        },
        { progressKind: "active_delay", successMessage: "All in-band delays reset" }
      );
    }

    function applyRelativeDelayEditorUpdate() {
      const antennaIndex = Math.max(0, parseInt(relativeDelayDraft.ant || "1", 10) - 1);
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/relative-delay/update",
            {
              session_id: sessionId,
              antenna: antennaIndex,
              x_delay_ns: relativeDelayDraft.x === "" ? null : parseFloat(relativeDelayDraft.x),
              y_delay_ns: relativeDelayDraft.y === "" ? null : parseFloat(relativeDelayDraft.y),
            },
            antennaIndex
          );
        },
        { progressKind: "relative_delay", successMessage: "Relative-phase fit updated" }
      );
    }

    function resetRelativeDelayEditorAntenna() {
      const antennaIndex = Math.max(0, parseInt(relativeDelayDraft.ant || "1", 10) - 1);
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/relative-delay/reset",
            {
              session_id: sessionId,
              antenna: antennaIndex,
            },
            antennaIndex
          );
        },
        { progressKind: "relative_delay", successMessage: "Relative-phase fit reset" }
      );
    }

    function applyRelativeDelaySuggestion() {
      const antennaIndex = Math.max(0, parseInt(relativeDelayDraft.ant || "1", 10) - 1);
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/relative-delay/apply-suggestion",
            {
              session_id: sessionId,
              antenna: antennaIndex,
            },
            antennaIndex
          );
        },
        { progressKind: "relative_delay", successMessage: "Relative-phase suggestion applied" }
      );
    }

    function applyResidualInbandFit() {
      runAction(
        function () {
          return jsonFetch("/api/inband/apply-residual-fit", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              session_id: sessionId,
              targets: Object.values(stagedResidualMasks),
            }),
          }).then(function (next) {
            const nextState = next && next.state ? next.state : next;
            setState(nextState);
            setInteractionMessage("");
            syncDraft(nextState);
            if (next && next.overview_updates) {
              setOverviewData(function (current) {
                return mergeOverviewAntennaUpdates(current, next.overview_updates, null);
              });
            }
            clearStagedResidualSelection();
            return next;
          });
        },
        { progressKind: "active_delay", successMessage: "Residual in-band fit applied" }
      );
    }

    function toggleManualAntennaFlag(antennaIndex, flagged) {
      if (!sessionId) {
        return;
      }
      postJsonWithOverviewPatch(
        "/api/relative-phase/antenna-flag",
        {
          session_id: sessionId,
          antenna: antennaIndex,
          flagged: flagged,
        },
        antennaIndex
      )
        .then(function (next) {
          const nextState = next && next.state ? next.state : null;
          setInteractionMessage(
            nextState && nextState.status_message
              ? nextState.status_message
              : flagged
                ? "Antenna excluded"
                : "Antenna restored"
          );
        })
        .catch(function (err) {
          setError(err.message || String(err));
        });
    }

    function applyYxResidualThreshold() {
      const threshold = parseFloat(yxThresholdDraft);
      if (!Number.isFinite(threshold) || threshold < 0) {
        setError("Y-X residual RMS threshold must be a non-negative number.");
        return;
      }
      runAction(
        function () {
          return postJsonWithOverviewRefresh("/api/relative-phase/yx-threshold", {
            session_id: sessionId,
            value: threshold,
          });
        },
        { progressKind: "relative_delay", successMessage: "Y-X residual RMS threshold updated" }
      );
    }

    function applyResidualBandThreshold() {
      const threshold = parseFloat(residualThresholdDraft);
      if (!Number.isFinite(threshold) || threshold < 0) {
        setError("Residual bad-band threshold must be a non-negative number.");
        return;
      }
      runAction(
        function () {
          return jsonFetch("/api/inband/residual-threshold", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              session_id: sessionId,
              value: threshold,
            }),
          }).then(function (next) {
            const nextState = next && next.state ? next.state : next;
            setState(nextState);
            setInteractionMessage("");
            syncDraft(nextState);
            if (next && next.overview_updates) {
              setOverviewData(function (current) {
                return mergeOverviewAntennaUpdates(current, next.overview_updates, null);
              });
            }
            clearStagedResidualSelection();
            return next;
          });
        },
        { progressKind: "active_delay", successMessage: "Residual bad-band threshold updated" }
      );
    }

    function undoRelativeDelayEditor() {
      const antennaIndex = Math.max(0, parseInt(relativeDelayDraft.ant || "1", 10) - 1);
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/relative-delay/undo",
            {
              session_id: sessionId,
              antenna: antennaIndex,
            },
            antennaIndex
          );
        },
        { progressKind: "relative_delay", successMessage: "Relative-phase edit undone" }
      );
    }

    const canAnalyze = sessionId && selectedScanId() !== null;
    const canRefcal = canAnalyze;
    const canPhacal = canAnalyze && state && state.ref_scan_id !== null;
    const canCombine = checkedScanIds.length === 2;
    const selectedAntLabel = state ? state.selected_ant + 1 : "-";
    const selectedBandLabel = state ? state.selected_band + 1 : "-";
    const currentScanLabel =
      state && state.current_scan
        ? state.current_scan.kind + " " + (state.current_scan.scan_time || state.current_scan.timestamp_iso)
        : "No scan selected";
    const activeRefLabel =
      state && state.active_refcal ? state.active_refcal.scan_time || state.active_refcal.timestamp_iso : "None";
    const activeRelativeRef = state && state.active_refcal ? state.active_refcal : null;
    const hasRelativeSuggestion =
      !!(
        activeRelativeRef &&
        (
          Math.abs(Number(activeRelativeRef.x_suggested_relative_delay_ns || 0)) > 1e-9 ||
          Math.abs(Number(activeRelativeRef.y_suggested_relative_delay_ns || 0)) > 1e-9
        )
      );
    const canApplyResidualFit = !!(state && state.active_refcal);
    const selectedCellFlagSum =
      heatmapData &&
      !heatmapData.message &&
      state &&
      heatmapData.values &&
      heatmapData.values[state.selected_band] &&
      Number.isFinite(heatmapData.values[state.selected_band][state.selected_ant])
        ? formatNumber(Number(heatmapData.values[state.selected_band][state.selected_ant]))
        : "—";
    const scanCount = state && state.scans ? state.scans.length : 0;
    const scanlistMaxHeightPx = Math.min(320, Math.max(80, 28 + scanCount * 29));
    const metadataWarnings = state && state.scan_metadata_warnings ? state.scan_metadata_warnings : [];
    const selectedPendingTimeIntervals =
      state && timeHistoryData && !timeHistoryData.message
        ? stagedTimeIntervals
            .filter(function (item) {
              return Number(item.antenna) === Number(state.selected_ant) && Number(item.band) === Number(state.selected_band);
            })
            .sort(function (a, b) {
              return Number(a.start_jd) - Number(b.start_jd) || Number(a.end_jd) - Number(b.end_jd);
            })
            .map(function (item) {
              return {
                group_id: String(item.temp_id),
                scope: String(item.scope),
                scope_label: timeFlagScopeLabel(item.scope),
                source: "staged",
                pending: true,
                start_jd: Number(item.start_jd),
                end_jd: Number(item.end_jd),
                start_offset_min: (Number(item.start_jd) - Number(timeHistoryData.start_jd || 0)) * 24.0 * 60.0,
                end_offset_min: (Number(item.end_jd) - Number(timeHistoryData.start_jd || 0)) * 24.0 * 60.0,
                start_label: formatUtcTime(Number(item.start_jd)),
                end_label: formatUtcTime(Number(item.end_jd)),
              };
            })
        : [];
    const pendingFlagMapCells =
      state && state.current_layout
        ? Array.from(
            stagedTimeIntervals.reduce(function (acc, interval) {
              expandTimeFlagTargets(interval, state.current_layout).forEach(function (target) {
                acc.set(String(target.antenna) + ":" + String(target.band), target);
              });
              return acc;
            }, new Map()).values()
          )
        : [];

    return html`
      <div className="app-shell" ref=${shellRef}>
        <section className=${"toolbar panel" + (toolbarCollapsed ? " toolbar-collapsed" : "")} ref=${toolbarRef}>
          ${toolbarCollapsed
            ? activity
              ? html`
                  <div className=${"toolbar-collapsed-progress" + (activity.error ? " error" : "")}>
                    <div className="toolbar-collapsed-progress-top">
                      <span className="toolbar-status-title">${activity.title}</span>
                      <span className="toolbar-status-percent">${Math.max(0, Math.min(100, Math.round(activity.progress)))}%</span>
                    </div>
                    <div className="toolbar-collapsed-progress-stage">${activity.stage}</div>
                    <div className="activity-meter toolbar-collapsed-progress-meter">
                      <div
                        className="activity-meter-fill"
                        style=${{ width: Math.max(4, Math.min(100, activity.progress)) + "%" }}
                      ></div>
                    </div>
                  </div>
                `
              : null
            : html`<div className="toolbar-main">
            <div className="toolbar-column toolbar-left-column">
              <div className="toolbar-controls-row">
                <div className="field">
                  <label>Date</label>
                  <input type="date" value=${dateText} onChange=${function (event) { setDateText(event.target.value); }} />
                </div>
                <label className="field">
                  <input
                    type="checkbox"
                    checked=${!!(state && state.fix_drift)}
                    onChange=${function (event) {
                      runAction(function () {
                        return postJson("/api/settings/fix-drift", {
                          session_id: sessionId,
                          fix_drift: event.target.checked,
                        });
                      });
                    }}
                  />
                  Fix Phase Drift
                </label>
              </div>

              <div className="toolbar-session-grid">
                <div>
                  <strong>Selected Scan</strong>
                  <span>${currentScanLabel}</span>
                </div>
                <div>
                  <strong>Anchor Refcal</strong>
                  <span>${activeRefLabel}</span>
                </div>
                <div>
                  <strong>Antenna</strong>
                  <span>${selectedAntLabel}</span>
                </div>
                <div>
                  <strong>Band</strong>
                  <span>${selectedBandLabel}</span>
                </div>
              </div>

              <div className="toolbar-actions-row">
                <button
                  type="button"
                  className="btn-outline-blue"
                  disabled=${!canAnalyze || busy}
                  onClick=${function () {
                    runAction(
                      function () {
                        return postJson("/api/refcal/analyze", { session_id: sessionId, scan_id: selectedScanId() });
                      },
                      { progressKind: "refcal", successMessage: "Refcal analysis complete" }
                    );
                  }}
                >
                  Analyze Refcal
                </button>
                <button
                  type="button"
                  className="btn-dark-fill"
                  disabled=${!canRefcal || busy}
                  onClick=${function () {
                    runAction(function () {
                      return postJson("/api/refcal/select", { session_id: sessionId, scan_id: selectedScanId() });
                    });
                  }}
                >
                  Set Refcal
                </button>
                <button
                  type="button"
                  className="btn-filter-pill"
                  disabled=${!canCombine || busy}
                  onClick=${function () {
                    runAction(
                      function () {
                        return postJson("/api/refcal/combine", { session_id: sessionId, scan_ids: checkedScanIds });
                      },
                      { progressKind: "combine_refcal", successMessage: "Refcal combination complete" }
                    );
                  }}
                >
                  Combine 2 Refcals
                </button>
                <button
                  type="button"
                  className="btn-outline-blue"
                  disabled=${!canPhacal || busy}
                  onClick=${function () {
                    runAction(
                      function () {
                        return postJson("/api/phacal/analyze", { session_id: sessionId, scan_id: selectedScanId() });
                      },
                      { progressKind: "phacal", successMessage: "Phacal analysis complete" }
                    );
                  }}
                >
                  Analyze Phacal
                </button>
                <button
                  type="button"
                  className="btn-blue"
                  disabled=${!canAnalyze || busy}
                  onClick=${function () {
                    runAction(
                      function () {
                        return postJson("/api/save/sql", { session_id: sessionId, scan_id: selectedScanId() });
                      },
                      { progressKind: "save_sql", successMessage: "Saved scan to SQL" }
                    );
                  }}
                >
                  Save SQL
                </button>
                <button
                  type="button"
                  className="btn-outline-blue"
                  disabled=${!sessionId || busy}
                  onClick=${function () {
                    runAction(
                      function () {
                        return jsonFetch("/api/save/npz", {
                          method: "POST",
                          headers: { "Content-Type": "application/json" },
                          body: JSON.stringify({ session_id: sessionId, scan_ids: checkedScanIds }),
                        }).then(function (next) {
                          const nextState = next && next.state ? next.state : next;
                          setState(nextState);
                          syncDraft(nextState);
                          return next;
                        });
                      },
                      { progressKind: "save_npz", successMessage: "Daily NPZ bundle saved" }
                    );
                  }}
                >
                  Save NPZ
                </button>
              </div>

              <section className="toolbar-workflow-box">
                <h2>Workflow</h2>
                <p>Set one HI refcal as the canonical anchor, tune in-band and relative-phase fits, then analyze the HI phacals and save SQL/NPZ. Handle LO separately; if a second HI refcal is available, keep it optional and use it only as secondary anchor metadata.</p>
              </section>

              <div className=${"toolbar-status" + (activity ? " toolbar-status-progress" : "") + (activity && activity.error ? " error" : "")}>
                ${activity
                  ? html`
                      <div className="toolbar-status-top">
                        <span className="toolbar-status-title">${activity.title}</span>
                        <span className="toolbar-status-percent">${Math.max(0, Math.min(100, Math.round(activity.progress)))}%</span>
                      </div>
                      <div className="toolbar-status-stage">${activity.stage}</div>
                      <div className="activity-meter">
                        <div
                          className="activity-meter-fill"
                          style=${{ width: Math.max(4, Math.min(100, activity.progress)) + "%" }}
                        ></div>
                      </div>
                    `
                  : error
                    ? html`<span className="error">${error}</span>`
                    : interactionMessage || (state ? state.status_message : "Initializing session...")}
              </div>
            </div>

            <div className="toolbar-column toolbar-right-column">
              <div className="rail-title-row">
                <h2>Scans</h2>
                <div className="rail-title-meta">
                  ${metadataWarnings.length ? html`<span>${metadataWarnings[0]}</span>` : null}
                </div>
              </div>
              <div className="scanlist-scroll toolbar-scanlist-scroll" style=${{ maxHeight: scanlistMaxHeightPx + "px" }}>
                <table className="scan-table">
                  <thead>
                    <tr>
                      <th>Time</th>
                      <th>Feed</th>
                      <th>Source</th>
                      <th>Dur</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${state && state.scans
                      ? state.scans.map(function (scan) {
                          const cls = [
                            scan.selected ? "selected" : "",
                            scan.is_refcal ? "refcal" : "",
                            checkedScanIds.indexOf(scan.scan_id) >= 0 ? "checked" : "",
                          ].join(" ");
                          return html`
                            <tr
                              key=${scan.scan_id}
                              className=${cls}
                              style=${{ backgroundColor: scan.color }}
                              onClick=${function (event) {
                                onScanRowClick(event, scan.scan_id);
                              }}
                            >
                              <td>${scan.scan_time}</td>
                              <td>${String(scan.feed_kind || "unknown").toUpperCase()}</td>
                              <td>${scan.source}</td>
                              <td>${scan.duration_min.toFixed(1)}m</td>
                              <td>${scan.status}${scan.is_refcal ? " *R" : ""}</td>
                            </tr>
                          `;
                        })
                      : null}
                  </tbody>
                </table>
              </div>
            </div>
          </div>`}
          <button
            type="button"
            className=${"toolbar-chevron-toggle" + (toolbarCollapsed ? " collapsed" : "")}
            aria-label=${toolbarCollapsed ? "Show toolbar" : "Hide toolbar"}
            onClick=${function () {
              setToolbarCollapsed(function (current) {
                return !current;
              });
            }}
          >
            <span className="toolbar-chevron-glyph">»</span>
          </button>
        </section>

        <main className="content-grid" style=${paneMaxHeight ? { "--content-pane-max-height": paneMaxHeight + "px" } : null}>
          <aside className="control-rail panel">
            <section className="rail-section heatmap-box">
              <div className="rail-title-row">
                <h2>Flag Map</h2>
                <div className="rail-title-meta">
                  <span>${"Flag " + selectedCellFlagSum}</span>
                  <button
                    type="button"
                    className="btn-outline-blue"
                    disabled=${busy || !stagedTimeIntervals.length}
                    onClick=${applyStagedTimeFlags}
                  >
                    Apply Mask
                  </button>
                </div>
              </div>
              <${HeatmapPlot}
                data=${heatmapData}
                selectedAnt=${state ? state.selected_ant : null}
                selectedBand=${state ? state.selected_band : null}
                pendingCells=${pendingFlagMapCells}
                onSelect=${onHeatmapSelect}
              />
            </section>

            <section className="rail-section compact-th-box">
              <div className="rail-title-row">
                <h2>Time History</h2>
                <div className="rail-title-meta">
                  <span>${timeHistoryData && timeHistoryData.title ? String(timeHistoryData.title).replace(", ", " · ") : "Ant " + selectedAntLabel + " · Band " + selectedBandLabel}</span>
                </div>
              </div>
              <${CompactTimeHistoryPlot}
                data=${timeHistoryData}
                scope=${timeFlagScope}
                pendingIntervals=${selectedPendingTimeIntervals}
                onScopeChange=${setTimeFlagScope}
                onStageInterval=${stageTimeFlag}
                onApplyPending=${applyStagedTimeFlags}
                onDeletePendingInterval=${removeStagedTimeFlag}
                onDeleteInterval=${deleteTimeFlag}
                onStatus=${setInteractionMessage}
                busy=${busy}
              />
            </section>

            <section className="rail-section delay-box">
              <h2>Anchor Refcal Delay Editor</h2>
              ${state && state.active_refcal
                ? html`
                    <div className="delay-summary">
                      <span>${"Refcal: " + activeRefLabel}; ${"Selected antenna: " + delayDraft.ant}</span>
                    </div>
                    <div className="delay-grid delay-pair-grid">
                      <div className="delay-pair-row">
                        <label>
                          X Delay (ns)
                          <input
                            type="number"
                            step="0.1"
                            value=${delayDraft.x}
                            onChange=${function (event) {
                              setDelayDraft(Object.assign({}, delayDraft, { x: event.target.value }));
                            }}
                          />
                        </label>
                        <label>
                          Y Delay (ns)
                          <input
                            type="number"
                            step="0.1"
                            value=${delayDraft.y}
                            onChange=${function (event) {
                              setDelayDraft(Object.assign({}, delayDraft, { y: event.target.value }));
                            }}
                          />
                        </label>
                      </div>
                      <div className="delay-actions full">
                        <button
                          type="button"
                          disabled=${busy}
                          onClick=${function () {
                            applyDelayEditorUpdate();
                          }}
                        >
                          Apply
                        </button>
                        <button
                          type="button"
                          disabled=${busy}
                          onClick=${function () {
                            resetDelayEditorAntenna();
                          }}
                        >
                          Reset Ant
                        </button>
                        <button
                          type="button"
                          disabled=${busy}
                          onClick=${function () {
                            resetAllDelayEditor();
                          }}
                        >
                          Reset All
                        </button>
                      </div>
                      <div className="full tiny">
                        ${state.active_refcal.dirty_inband
                          ? "Active refcal has unsaved in-band edits."
                          : "Active refcal matches the fitted in-band solution."}
                      </div>
                    </div>
                  `
                : html`
                    <div className="delay-empty">
                      <div className="delay-summary">
                        <span>Refcal: None; ${"Selected antenna: " + delayDraft.ant}</span>
                      </div>
                      <div className="tiny">Set a refcal to edit per-antenna X/Y in-band delays.</div>
                    </div>
                  `}
            </section>

            <section className="rail-section delay-box">
              <h2>Relative Phase Delay Editor</h2>
              ${state && state.active_refcal
                ? html`
                    <div className="delay-summary">
                      <span>${"Refcal: " + activeRefLabel}; ${"Selected antenna: " + relativeDelayDraft.ant}</span>
                    </div>
                    <div className="delay-grid delay-pair-grid">
                      <div className="delay-pair-row">
                        <label>
                          X Auto Baseline (ns)
                          <input type="text" disabled value=${formatNumber(Number(state.active_refcal.x_auto_relative_delay_ns || 0))} />
                        </label>
                        <label>
                          Y Auto Baseline (ns)
                          <input type="text" disabled value=${formatNumber(Number(state.active_refcal.y_auto_relative_delay_ns || 0))} />
                        </label>
                      </div>
                      <div className="delay-pair-row">
                        <label>
                          X Suggested Corr. (ns)
                          <input type="text" disabled value=${formatNumber(Number(state.active_refcal.x_suggested_relative_delay_ns || 0))} />
                        </label>
                        <label>
                          Y Suggested Corr. (ns)
                          <input type="text" disabled value=${formatNumber(Number(state.active_refcal.y_suggested_relative_delay_ns || 0))} />
                        </label>
                      </div>
                      <div className="delay-pair-row">
                        <label>
                          X Applied Corr. (ns)
                          <input
                            type="number"
                            step="0.1"
                            value=${relativeDelayDraft.x}
                            onChange=${function (event) {
                              setRelativeDelayDraft(Object.assign({}, relativeDelayDraft, { x: event.target.value }));
                            }}
                          />
                        </label>
                        <label>
                          Y Applied Corr. (ns)
                          <input
                            type="number"
                            step="0.1"
                            value=${relativeDelayDraft.y}
                            onChange=${function (event) {
                              setRelativeDelayDraft(Object.assign({}, relativeDelayDraft, { y: event.target.value }));
                            }}
                          />
                        </label>
                      </div>
                      <div className="delay-pair-row">
                        <label>
                          X Effective Fit (ns)
                          <input type="text" disabled value=${formatNumber(Number(state.active_refcal.x_effective_relative_delay_ns || 0))} />
                        </label>
                        <label>
                          Y Effective Fit (ns)
                          <input type="text" disabled value=${formatNumber(Number(state.active_refcal.y_effective_relative_delay_ns || 0))} />
                        </label>
                      </div>
                      <div className="delay-actions full">
                        <button
                          type="button"
                          disabled=${busy}
                          onClick=${function () {
                            applyRelativeDelayEditorUpdate();
                          }}
                        >
                          Apply
                        </button>
                        <button
                          type="button"
                          disabled=${busy || !hasRelativeSuggestion}
                          onClick=${function () {
                            applyRelativeDelaySuggestion();
                          }}
                        >
                          Apply Suggestion
                        </button>
                        <button
                          type="button"
                          disabled=${busy || !(state.active_refcal && state.active_refcal.relative_undo_available)}
                          onClick=${function () {
                            undoRelativeDelayEditor();
                          }}
                        >
                          Undo Last
                        </button>
                        <button
                          type="button"
                          disabled=${busy}
                          onClick=${function () {
                            resetRelativeDelayEditorAntenna();
                          }}
                        >
                          Reset Auto
                        </button>
                      </div>
                      <div className="full tiny">
                        Auto baseline comes from kept-band fitting. Suggested correction is residual-guided and only applies when clicked.
                      </div>
                    </div>
                  `
                : html`
                    <div className="delay-empty">
                      <div className="delay-summary">
                        <span>Refcal: None; ${"Selected antenna: " + relativeDelayDraft.ant}</span>
                      </div>
                      <div className="tiny">Set or analyze a refcal to edit per-antenna X/Y relative residual delays.</div>
                    </div>
                  `}
            </section>
          </aside>

          <section className="analysis-wall panel">
            <div className="analysis-stack">
              ${OVERVIEW_SECTIONS.map(function (section) {
                const plotData = overviewData ? overviewData[section.id] : null;
                return html`
                  <${PlotCard}
                    key=${section.id}
                    title=${section.label}
                    legend=${section.showLegend === false ? null : plotData && plotData.legend}
                    inlineControls=${section.id === "inband_fit"
                      ? html`<${InbandWindowControls}
                          antennaScope=${inbandAntennaScope}
                          polScope=${inbandPolScope}
                          busy=${busy}
                          hasPending=${Object.keys(stagedInbandMasks).length > 0}
                          pendingCount=${Object.keys(stagedInbandMasks).length}
                          onAntennaScopeChange=${setInbandAntennaScope}
                          onPolScopeChange=${setInbandPolScope}
                          onApply=${applyStagedInbandWindow}
                        />`
                      : section.id === "inband_relative_phase"
                        ? html`
                            <${InbandWindowControls}
                              antennaScope=${inbandAntennaScope}
                              polScope=${inbandPolScope}
                              busy=${busy}
                              hasPending=${Object.keys(stagedInbandMasks).length > 0}
                              pendingCount=${Object.keys(stagedInbandMasks).length}
                              onAntennaScopeChange=${setInbandAntennaScope}
                              onPolScopeChange=${setInbandPolScope}
                              onApply=${applyStagedInbandWindow}
                            />
                            <div className="plot-inline-threshold">
                              <label>
                                <span>Y-X RMS Threshold (rad)</span>
                                <input
                                  type="number"
                                  step="0.1"
                                  value=${yxThresholdDraft}
                                  disabled=${busy || !(state && state.active_refcal)}
                                  onChange=${function (event) {
                                    setYxThresholdDraft(event.target.value);
                                  }}
                                />
                              </label>
                              <button
                                type="button"
                                className="btn-outline-blue"
                                disabled=${busy || !(state && state.active_refcal)}
                                onClick=${function () {
                                  applyYxResidualThreshold();
                                }}
                              >
                                Apply
                              </button>
                              <span className="plot-inline-summary">
                                ${state && state.active_refcal
                                  ? "Y-X residual RMS: "
                                    + formatNumber(Number(state.active_refcal.yx_residual_rms || 0))
                                    + " rad (threshold "
                                    + formatNumber(Number(state.active_refcal.yx_residual_threshold_rad || 1.5))
                                    + ")"
                                  : "No active refcal"}
                              </span>
                            </div>
                          `
                      : section.id === "inband_residual_phase_band"
                        ? html`
                            <div className="plot-inline-threshold">
                              <label>
                                <span>Residual Band Threshold (rad)</span>
                                <input
                                  type="number"
                                  step="0.1"
                                  value=${residualThresholdDraft}
                                  disabled=${busy || !(state && state.active_refcal)}
                                  onChange=${function (event) {
                                    setResidualThresholdDraft(event.target.value);
                                  }}
                                />
                              </label>
                              <button
                                type="button"
                                className="btn-outline-blue"
                                disabled=${busy || !(state && state.active_refcal)}
                                onClick=${function () {
                                  applyResidualBandThreshold();
                                }}
                              >
                                Apply Threshold
                              </button>
                              <button
                                type="button"
                                className="btn-outline-blue"
                                disabled=${busy || !canApplyResidualFit}
                                onClick=${function () {
                                  applyResidualInbandFit();
                                }}
                              >
                                Apply Residual Fit
                              </button>
                            </div>
                          `
                      : null}
                  >
                    <${PanelGridPlot}
                      data=${plotData}
                      hideLegend=${true}
                      busy=${busy}
                      panelHeight=${section.panelHeight}
                      interactionMode=${section.id === "inband_residual_delay_band" ? "zoom" : null}
                      panelOverride=${section.id === "inband_fit" || section.id === "inband_relative_phase"
                        ? function (rowIdx, panelIdx, panel) {
                            return panelWithStagedInbandSelection(section.id, rowIdx, panelIdx, panel);
                          }
                        : section.id === "inband_residual_phase_band"
                          ? function (rowIdx, panelIdx, panel) {
                              return panelWithStagedResidualSelection(rowIdx, panelIdx, panel);
                            }
                        : null}
                      onBandWindowSelect=${section.id === "inband_fit" || section.id === "inband_relative_phase"
                        ? function (rowIdx, panelIdx, startBand, endBand, mode) {
                            stageInbandWindow(startBand, endBand, rowIdx, panelIdx, mode);
                          }
                        : section.id === "inband_residual_phase_band"
                          ? function (rowIdx, panelIdx, startBand, endBand) {
                              stageResidualSelection(startBand, endBand, rowIdx, panelIdx);
                            }
                        : null}
                      onPanelDoubleClick=${section.id === "inband_fit" || section.id === "inband_relative_phase"
                        ? function (rowIdx, panelIdx) {
                            stageInbandMaskClear(rowIdx, panelIdx);
                          }
                        : section.id === "inband_residual_phase_band"
                          ? function (rowIdx, panelIdx) {
                              stageResidualMaskClear(rowIdx, panelIdx);
                            }
                        : null}
                      bandSelectApplyLabel=${section.id === "inband_residual_phase_band" ? "Apply Residual Fit" : "Apply Mask"}
                      onColumnToggle=${section.id === "inband_relative_phase"
                        ? function (antennaIndex, flagged) {
                            toggleManualAntennaFlag(antennaIndex, flagged);
                          }
                        : null}
                    />
                  </${PlotCard}>
                `;
              })}
            </div>
          </section>
        </main>

        <${ResidualInspectorModal}
          data=${inbandResidualInspector && inbandResidualInspector.data}
          title=${(inbandResidualInspector && inbandResidualInspector.title) || "Inband Fit Residuals"}
          onClose=${function () {
            setInbandResidualInspector(null);
          }}
        />

      </div>
    `;
  }

  ReactDOM.createRoot(document.getElementById("app")).render(html`<${App} />`);
})();
